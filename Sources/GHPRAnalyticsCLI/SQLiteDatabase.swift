import Foundation
import SQLite3

private let sqliteTransient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

enum SQLiteValue {
    case integer(Int64)
    case text(String)
    case null
}

final class SQLiteDatabase: AnalyticsStore {
    private let databasePointer: OpaquePointer

    init(path: String) throws {
        let pathURL = URL(fileURLWithPath: path)
        let directoryURL = pathURL.deletingLastPathComponent()

        if !FileManager.default.fileExists(atPath: directoryURL.path) {
            try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        }

        var pointer: OpaquePointer?
        if sqlite3_open(path, &pointer) != SQLITE_OK {
            defer { sqlite3_close(pointer) }
            throw AnalyticsError.message("Unable to open SQLite database at \(path)")
        }

        guard let pointer else {
            throw AnalyticsError.message("Unable to initialize SQLite database at \(path)")
        }

        databasePointer = pointer

        try execute(sql: "PRAGMA journal_mode=WAL;")
        try execute(sql: "PRAGMA synchronous=NORMAL;")
        try execute(sql: "PRAGMA foreign_keys=ON;")
    }

    deinit {
        sqlite3_close(databasePointer)
    }

    func migrate() throws {
        try execute(
            sql: """
            CREATE TABLE IF NOT EXISTS pull_requests (
                repo TEXT NOT NULL,
                number INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                closed_at TEXT,
                merged_at TEXT,
                state TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                first_approval_at TEXT,
                first_approval_review_id INTEGER,
                first_approval_actor TEXT,
                review_scan_state TEXT NOT NULL DEFAULT 'pending',
                PRIMARY KEY (repo, number)
            );
            """
        )

        try execute(
            sql: """
            CREATE TABLE IF NOT EXISTS reviews (
                repo TEXT NOT NULL,
                review_id INTEGER NOT NULL,
                pull_request_number INTEGER NOT NULL,
                state TEXT NOT NULL,
                submitted_at TEXT,
                author_login TEXT,
                PRIMARY KEY (repo, review_id),
                FOREIGN KEY (repo, pull_request_number) REFERENCES pull_requests(repo, number)
            );
            """
        )

        try execute(
            sql: """
            CREATE TABLE IF NOT EXISTS sync_state (
                repo TEXT NOT NULL,
                phase TEXT NOT NULL,
                mode TEXT NOT NULL,
                cursor TEXT,
                last_success_at TEXT,
                watermark_updated_at TEXT,
                status TEXT,
                error TEXT,
                PRIMARY KEY (repo, phase, mode)
            );
            """
        )

        try execute(sql: "CREATE INDEX IF NOT EXISTS index_pull_requests_created_at ON pull_requests(repo, created_at);")
        try execute(sql: "CREATE INDEX IF NOT EXISTS index_pull_requests_merged_at ON pull_requests(repo, merged_at);")
        try execute(sql: "CREATE INDEX IF NOT EXISTS index_pull_requests_updated_at ON pull_requests(repo, updated_at);")
        try execute(sql: "CREATE INDEX IF NOT EXISTS index_reviews_pull_request_number ON reviews(repo, pull_request_number);")
    }

    func execute(sql: String, bindings: [SQLiteValue] = []) throws {
        let statement = try prepare(sql: sql)
        defer { sqlite3_finalize(statement) }

        try bind(values: bindings, to: statement)

        let result = sqlite3_step(statement)
        guard result == SQLITE_DONE || result == SQLITE_ROW else {
            throw SQLiteDatabase.errorMessage(databasePointer, prefix: "SQL execution failed")
        }
    }

    func inTransaction(_ body: () throws -> Void) throws {
        try execute(sql: "BEGIN IMMEDIATE;")

        do {
            try body()
            try execute(sql: "COMMIT;")
        } catch {
            try? execute(sql: "ROLLBACK;")
            throw error
        }
    }

    func query(sql: String, bindings: [SQLiteValue] = []) throws -> [[String: String?]] {
        let statement = try prepare(sql: sql)
        defer { sqlite3_finalize(statement) }

        try bind(values: bindings, to: statement)

        var rows: [[String: String?]] = []

        while true {
            let result = sqlite3_step(statement)

            if result == SQLITE_DONE {
                break
            }

            guard result == SQLITE_ROW else {
                throw SQLiteDatabase.errorMessage(databasePointer, prefix: "SQL query failed")
            }

            var row: [String: String?] = [:]
            let columnCount = sqlite3_column_count(statement)

            for index in 0 ..< columnCount {
                let columnName = String(cString: sqlite3_column_name(statement, index))
                let columnType = sqlite3_column_type(statement, index)

                switch columnType {
                case SQLITE_NULL:
                    row[columnName] = nil
                case SQLITE_INTEGER:
                    row[columnName] = String(sqlite3_column_int64(statement, index))
                case SQLITE_FLOAT:
                    row[columnName] = String(sqlite3_column_double(statement, index))
                case SQLITE_TEXT:
                    if let pointer = sqlite3_column_text(statement, index) {
                        row[columnName] = String(cString: pointer)
                    } else {
                        row[columnName] = nil
                    }
                default:
                    row[columnName] = nil
                }
            }

            rows.append(row)
        }

        return rows
    }

    func upsertPullRequest(repository: String, metadata: PullRequestMetadata, reviewScanState: String = "pending") throws {
        try execute(
            sql: """
            INSERT INTO pull_requests (
                repo,
                number,
                created_at,
                closed_at,
                merged_at,
                state,
                updated_at,
                review_scan_state
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo, number) DO UPDATE SET
                created_at = excluded.created_at,
                closed_at = excluded.closed_at,
                merged_at = excluded.merged_at,
                state = excluded.state,
                updated_at = excluded.updated_at,
                review_scan_state = CASE
                    WHEN pull_requests.updated_at <> excluded.updated_at THEN 'pending'
                    ELSE pull_requests.review_scan_state
                END;
            """,
            bindings: [
                .text(repository),
                .integer(Int64(metadata.number)),
                .text(metadata.createdAt),
                metadata.closedAt.map(SQLiteValue.text) ?? .null,
                metadata.mergedAt.map(SQLiteValue.text) ?? .null,
                .text(metadata.state),
                .text(metadata.updatedAt),
                .text(reviewScanState)
            ]
        )
    }

    func upsertReview(repository: String, pullRequestNumber: Int, review: PullRequestReview) throws {
        try execute(
            sql: """
            INSERT INTO reviews (
                repo,
                review_id,
                pull_request_number,
                state,
                submitted_at,
                author_login
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo, review_id) DO UPDATE SET
                pull_request_number = excluded.pull_request_number,
                state = excluded.state,
                submitted_at = excluded.submitted_at,
                author_login = excluded.author_login;
            """,
            bindings: [
                .text(repository),
                .integer(review.reviewIdentifier),
                .integer(Int64(pullRequestNumber)),
                .text(review.state),
                review.submittedAt.map(SQLiteValue.text) ?? .null,
                review.user?.login.map(SQLiteValue.text) ?? .null
            ]
        )
    }

    func setFirstApproval(repository: String, pullRequestNumber: Int, approvalEvent: ApprovalEvent?) throws {
        try execute(
            sql: """
            UPDATE pull_requests
            SET
                first_approval_at = ?,
                first_approval_review_id = ?,
                first_approval_actor = ?,
                review_scan_state = 'complete'
            WHERE repo = ? AND number = ?;
            """,
            bindings: [
                approvalEvent.map { SQLiteValue.text($0.submittedAt) } ?? .null,
                approvalEvent?.reviewIdentifier.map { SQLiteValue.integer($0) } ?? .null,
                approvalEvent?.actor.map { SQLiteValue.text($0) } ?? .null,
                .text(repository),
                .integer(Int64(pullRequestNumber))
            ]
        )
    }

    func fetchSyncState(repository: String, phase: String, mode: String) throws -> SyncStateRecord? {
        let rows = try query(
            sql: """
            SELECT phase, mode, cursor, last_success_at, watermark_updated_at, status, error
            FROM sync_state
            WHERE repo = ? AND phase = ? AND mode = ?;
            """,
            bindings: [.text(repository), .text(phase), .text(mode)]
        )

        guard let row = rows.first else {
            return nil
        }

        return SyncStateRecord(
            phase: value(for: "phase", in: row) ?? phase,
            mode: value(for: "mode", in: row) ?? mode,
            cursor: value(for: "cursor", in: row),
            lastSuccessAt: value(for: "last_success_at", in: row),
            watermarkUpdatedAt: value(for: "watermark_updated_at", in: row),
            status: value(for: "status", in: row),
            error: value(for: "error", in: row)
        )
    }

    func upsertSyncState(
        repository: String,
        phase: String,
        mode: String,
        cursor: String?,
        lastSuccessAt: String?,
        watermarkUpdatedAt: String?,
        status: String?,
        error: String?
    ) throws {
        try execute(
            sql: """
            INSERT INTO sync_state (
                repo,
                phase,
                mode,
                cursor,
                last_success_at,
                watermark_updated_at,
                status,
                error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repo, phase, mode) DO UPDATE SET
                cursor = excluded.cursor,
                last_success_at = excluded.last_success_at,
                watermark_updated_at = excluded.watermark_updated_at,
                status = excluded.status,
                error = excluded.error;
            """,
            bindings: [
                .text(repository),
                .text(phase),
                .text(mode),
                cursor.map(SQLiteValue.text) ?? .null,
                lastSuccessAt.map(SQLiteValue.text) ?? .null,
                watermarkUpdatedAt.map(SQLiteValue.text) ?? .null,
                status.map(SQLiteValue.text) ?? .null,
                error.map(SQLiteValue.text) ?? .null
            ]
        )
    }

    func fetchReviewCandidates(
        repository: String,
        backfill: Bool,
        watermarkUpdatedAt: String?,
        dateBounds: DateBounds,
        skipClosedUnmergedPullRequests: Bool
    ) throws -> [ReviewCandidate] {
        var sql = "SELECT number, updated_at, created_at FROM pull_requests WHERE repo = ?"
        var bindings: [SQLiteValue] = [.text(repository)]

        if !backfill {
            if let watermarkUpdatedAt {
                sql += " AND (review_scan_state != 'complete' OR updated_at > ?)"
                bindings.append(.text(watermarkUpdatedAt))
            } else {
                sql += " AND review_scan_state != 'complete'"
            }
        }

        if let fromDate = Timestamp.format(dateBounds.from) {
            sql += " AND created_at >= ?"
            bindings.append(.text(fromDate))
        }

        if let toDate = Timestamp.format(dateBounds.to) {
            sql += " AND created_at <= ?"
            bindings.append(.text(toDate))
        }

        if skipClosedUnmergedPullRequests {
            sql += " AND (state = 'open' OR merged_at IS NOT NULL)"
        }

        sql += " ORDER BY number ASC;"

        let rows = try query(sql: sql, bindings: bindings)

        return rows.compactMap { row in
            guard
                let numberString = value(for: "number", in: row),
                let number = Int(numberString),
                let updatedAt = value(for: "updated_at", in: row),
                let createdAt = value(for: "created_at", in: row)
            else {
                return nil
            }

            return ReviewCandidate(number: number, updatedAt: updatedAt, createdAt: createdAt)
        }
    }

    func fetchAllPullRequests(repository: String) throws -> [PullRequestSnapshot] {
        let rows = try query(
            sql: """
            SELECT
                number,
                created_at,
                closed_at,
                merged_at,
                state,
                updated_at,
                first_approval_at,
                first_approval_review_id,
                first_approval_actor,
                review_scan_state
            FROM pull_requests
            WHERE repo = ?
            ORDER BY number ASC;
            """,
            bindings: [.text(repository)]
        )

        return rows.compactMap { row in
            guard
                let numberString = value(for: "number", in: row),
                let number = Int(numberString),
                let createdAt = value(for: "created_at", in: row),
                let state = value(for: "state", in: row),
                let updatedAt = value(for: "updated_at", in: row)
            else {
                return nil
            }

            return PullRequestSnapshot(
                number: number,
                createdAt: createdAt,
                closedAt: value(for: "closed_at", in: row),
                mergedAt: value(for: "merged_at", in: row),
                state: state,
                updatedAt: updatedAt,
                firstApprovalAt: value(for: "first_approval_at", in: row),
                firstApprovalReviewIdentifier: value(for: "first_approval_review_id", in: row).flatMap(Int64.init),
                firstApprovalActor: value(for: "first_approval_actor", in: row),
                reviewScanState: value(for: "review_scan_state", in: row) ?? "pending"
            )
        }
    }

    func fetchAllReviews(repository: String) throws -> [PullRequestReviewSnapshot] {
        let rows = try query(
            sql: """
            SELECT review_id, pull_request_number, state, submitted_at, author_login
            FROM reviews
            WHERE repo = ?
            ORDER BY pull_request_number ASC, review_id ASC;
            """,
            bindings: [.text(repository)]
        )

        return rows.compactMap { row in
            guard
                let reviewIdentifierString = value(for: "review_id", in: row),
                let reviewIdentifier = Int64(reviewIdentifierString),
                let pullRequestNumberString = value(for: "pull_request_number", in: row),
                let pullRequestNumber = Int(pullRequestNumberString),
                let state = value(for: "state", in: row)
            else {
                return nil
            }

            return PullRequestReviewSnapshot(
                reviewIdentifier: reviewIdentifier,
                pullRequestNumber: pullRequestNumber,
                state: state,
                submittedAt: value(for: "submitted_at", in: row),
                authorLogin: value(for: "author_login", in: row)
            )
        }
    }

    func listSyncStates(repository: String) throws -> [SyncStateRecord] {
        let rows = try query(
            sql: """
            SELECT phase, mode, cursor, last_success_at, watermark_updated_at, status, error
            FROM sync_state
            WHERE repo = ?
            ORDER BY phase ASC, mode ASC;
            """,
            bindings: [.text(repository)]
        )

        return rows.map { row in
            SyncStateRecord(
                phase: value(for: "phase", in: row) ?? "",
                mode: value(for: "mode", in: row) ?? "",
                cursor: value(for: "cursor", in: row),
                lastSuccessAt: value(for: "last_success_at", in: row),
                watermarkUpdatedAt: value(for: "watermark_updated_at", in: row),
                status: value(for: "status", in: row),
                error: value(for: "error", in: row)
            )
        }
    }

    func pullRequestCount(repository: String) throws -> Int {
        let rows = try query(
            sql: "SELECT COUNT(*) AS count FROM pull_requests WHERE repo = ?;",
            bindings: [.text(repository)]
        )

        guard let row = rows.first, let countString = value(for: "count", in: row), let count = Int(countString) else {
            return 0
        }

        return count
    }

    func missingApprovalCount(repository: String) throws -> Int {
        let rows = try query(
            sql: "SELECT COUNT(*) AS count FROM pull_requests WHERE repo = ? AND first_approval_at IS NULL;",
            bindings: [.text(repository)]
        )

        guard let row = rows.first, let countString = value(for: "count", in: row), let count = Int(countString) else {
            return 0
        }

        return count
    }

    func maximumUpdatedAt(repository: String) throws -> String? {
        let rows = try query(
            sql: "SELECT MAX(updated_at) AS maximum_updated_at FROM pull_requests WHERE repo = ?;",
            bindings: [.text(repository)]
        )

        guard let row = rows.first else {
            return nil
        }
        return value(for: "maximum_updated_at", in: row)
    }

    private func prepare(sql: String) throws -> OpaquePointer {
        var statement: OpaquePointer?

        guard sqlite3_prepare_v2(databasePointer, sql, -1, &statement, nil) == SQLITE_OK else {
            throw SQLiteDatabase.errorMessage(databasePointer, prefix: "SQL prepare failed")
        }

        guard let statement else {
            throw AnalyticsError.message("Unable to create SQL statement")
        }

        return statement
    }

    private func bind(values: [SQLiteValue], to statement: OpaquePointer) throws {
        for (index, value) in values.enumerated() {
            let parameterIndex = Int32(index + 1)

            let result: Int32
            switch value {
            case let .integer(integerValue):
                result = sqlite3_bind_int64(statement, parameterIndex, integerValue)
            case let .text(stringValue):
                result = sqlite3_bind_text(statement, parameterIndex, stringValue, -1, sqliteTransient)
            case .null:
                result = sqlite3_bind_null(statement, parameterIndex)
            }

            guard result == SQLITE_OK else {
                throw SQLiteDatabase.errorMessage(databasePointer, prefix: "SQL bind failed")
            }
        }
    }

    private func value(for key: String, in row: [String: String?]) -> String? {
        row[key] ?? nil
    }

    private static func errorMessage(_ pointer: OpaquePointer, prefix: String) -> AnalyticsError {
        let sqliteMessage = sqlite3_errmsg(pointer).map { String(cString: $0) } ?? "Unknown SQLite error"
        return .message("\(prefix): \(sqliteMessage)")
    }
}

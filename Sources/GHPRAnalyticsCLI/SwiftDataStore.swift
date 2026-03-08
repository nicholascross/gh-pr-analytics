import Foundation
import SwiftData

@Model
final class PullRequestEntity {
    @Attribute(.unique) var identifier: String
    var repository: String
    var number: Int
    var createdAt: String
    var closedAt: String?
    var mergedAt: String?
    var state: String
    var updatedAt: String
    var firstApprovalAt: String?
    var firstApprovalReviewIdentifier: Int64?
    var firstApprovalActor: String?
    var reviewScanState: String

    init(
        identifier: String,
        repository: String,
        number: Int,
        createdAt: String,
        closedAt: String?,
        mergedAt: String?,
        state: String,
        updatedAt: String,
        firstApprovalAt: String?,
        firstApprovalReviewIdentifier: Int64?,
        firstApprovalActor: String?,
        reviewScanState: String
    ) {
        self.identifier = identifier
        self.repository = repository
        self.number = number
        self.createdAt = createdAt
        self.closedAt = closedAt
        self.mergedAt = mergedAt
        self.state = state
        self.updatedAt = updatedAt
        self.firstApprovalAt = firstApprovalAt
        self.firstApprovalReviewIdentifier = firstApprovalReviewIdentifier
        self.firstApprovalActor = firstApprovalActor
        self.reviewScanState = reviewScanState
    }
}

@Model
final class PullRequestReviewEntity {
    @Attribute(.unique) var identifier: String
    var repository: String
    var reviewIdentifier: Int64
    var pullRequestNumber: Int
    var state: String
    var submittedAt: String?
    var authorLogin: String?

    init(
        identifier: String,
        repository: String,
        reviewIdentifier: Int64,
        pullRequestNumber: Int,
        state: String,
        submittedAt: String?,
        authorLogin: String?
    ) {
        self.identifier = identifier
        self.repository = repository
        self.reviewIdentifier = reviewIdentifier
        self.pullRequestNumber = pullRequestNumber
        self.state = state
        self.submittedAt = submittedAt
        self.authorLogin = authorLogin
    }
}

@Model
final class SyncStateEntity {
    @Attribute(.unique) var identifier: String
    var repository: String
    var phase: String
    var mode: String
    var cursor: String?
    var lastSuccessAt: String?
    var watermarkUpdatedAt: String?
    var status: String?
    var error: String?

    init(
        identifier: String,
        repository: String,
        phase: String,
        mode: String,
        cursor: String?,
        lastSuccessAt: String?,
        watermarkUpdatedAt: String?,
        status: String?,
        error: String?
    ) {
        self.identifier = identifier
        self.repository = repository
        self.phase = phase
        self.mode = mode
        self.cursor = cursor
        self.lastSuccessAt = lastSuccessAt
        self.watermarkUpdatedAt = watermarkUpdatedAt
        self.status = status
        self.error = error
    }
}

final class SwiftDataStore: AnalyticsStore {
    private let modelContainer: ModelContainer
    private var transactionContext: ModelContext?

    init(path: String) throws {
        let storeURL = URL(fileURLWithPath: path)
        let directoryURL = storeURL.deletingLastPathComponent()

        if !FileManager.default.fileExists(atPath: directoryURL.path) {
            try FileManager.default.createDirectory(at: directoryURL, withIntermediateDirectories: true)
        }

        let configuration = ModelConfiguration(url: storeURL)
        modelContainer = try ModelContainer(
            for: PullRequestEntity.self,
            PullRequestReviewEntity.self,
            SyncStateEntity.self,
            configurations: configuration
        )
    }

    func migrate() throws {}

    func inTransaction(_ body: () throws -> Void) throws {
        if transactionContext != nil {
            try body()
            return
        }

        let context = ModelContext(modelContainer)
        transactionContext = context

        do {
            try body()
            if context.hasChanges {
                try context.save()
            }
            transactionContext = nil
        } catch {
            transactionContext = nil
            throw error
        }
    }

    func upsertPullRequest(
        repository: String,
        metadata: PullRequestMetadata,
        reviewScanState: String = "pending"
    ) throws {
        let context = currentContext()
        let recordIdentifier = pullRequestIdentifier(repository: repository, number: metadata.number)

        if let existingRecord = try fetchPullRequest(identifier: recordIdentifier, context: context) {
            let previousUpdatedAt = existingRecord.updatedAt
            existingRecord.createdAt = metadata.createdAt
            existingRecord.closedAt = metadata.closedAt
            existingRecord.mergedAt = metadata.mergedAt
            existingRecord.state = metadata.state
            existingRecord.updatedAt = metadata.updatedAt
            if previousUpdatedAt != metadata.updatedAt {
                existingRecord.reviewScanState = "pending"
            }
        } else {
            let record = PullRequestEntity(
                identifier: recordIdentifier,
                repository: repository,
                number: metadata.number,
                createdAt: metadata.createdAt,
                closedAt: metadata.closedAt,
                mergedAt: metadata.mergedAt,
                state: metadata.state,
                updatedAt: metadata.updatedAt,
                firstApprovalAt: nil,
                firstApprovalReviewIdentifier: nil,
                firstApprovalActor: nil,
                reviewScanState: reviewScanState
            )
            context.insert(record)
        }

        try saveIfNeeded(context)
    }

    func upsertReview(repository: String, pullRequestNumber: Int, review: PullRequestReview) throws {
        let context = currentContext()
        let recordIdentifier = reviewIdentifier(repository: repository, reviewIdentifier: review.reviewIdentifier)

        if let existingRecord = try fetchReview(identifier: recordIdentifier, context: context) {
            existingRecord.pullRequestNumber = pullRequestNumber
            existingRecord.state = review.state
            existingRecord.submittedAt = review.submittedAt
            existingRecord.authorLogin = review.user?.login
        } else {
            let record = PullRequestReviewEntity(
                identifier: recordIdentifier,
                repository: repository,
                reviewIdentifier: review.reviewIdentifier,
                pullRequestNumber: pullRequestNumber,
                state: review.state,
                submittedAt: review.submittedAt,
                authorLogin: review.user?.login
            )
            context.insert(record)
        }

        try saveIfNeeded(context)
    }

    func setFirstApproval(repository: String, pullRequestNumber: Int, approvalEvent: ApprovalEvent?) throws {
        let context = currentContext()
        let recordIdentifier = pullRequestIdentifier(repository: repository, number: pullRequestNumber)

        guard let record = try fetchPullRequest(identifier: recordIdentifier, context: context) else {
            return
        }

        record.firstApprovalAt = approvalEvent?.submittedAt
        record.firstApprovalReviewIdentifier = approvalEvent?.reviewIdentifier
        record.firstApprovalActor = approvalEvent?.actor
        record.reviewScanState = "complete"

        try saveIfNeeded(context)
    }

    func fetchSyncState(repository: String, phase: String, mode: String) throws -> SyncStateRecord? {
        let context = currentContext()
        let recordIdentifier = syncStateIdentifier(repository: repository, phase: phase, mode: mode)
        let record = try fetchSyncState(identifier: recordIdentifier, context: context)

        guard let record else {
            return nil
        }

        return SyncStateRecord(
            phase: record.phase,
            mode: record.mode,
            cursor: record.cursor,
            lastSuccessAt: record.lastSuccessAt,
            watermarkUpdatedAt: record.watermarkUpdatedAt,
            status: record.status,
            error: record.error
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
        let context = currentContext()
        let recordIdentifier = syncStateIdentifier(repository: repository, phase: phase, mode: mode)

        if let existingRecord = try fetchSyncState(identifier: recordIdentifier, context: context) {
            existingRecord.cursor = cursor
            existingRecord.lastSuccessAt = lastSuccessAt
            existingRecord.watermarkUpdatedAt = watermarkUpdatedAt
            existingRecord.status = status
            existingRecord.error = error
        } else {
            let record = SyncStateEntity(
                identifier: recordIdentifier,
                repository: repository,
                phase: phase,
                mode: mode,
                cursor: cursor,
                lastSuccessAt: lastSuccessAt,
                watermarkUpdatedAt: watermarkUpdatedAt,
                status: status,
                error: error
            )
            context.insert(record)
        }

        try saveIfNeeded(context)
    }

    func fetchReviewCandidates(
        repository: String,
        backfill: Bool,
        watermarkUpdatedAt: String?,
        dateBounds: DateBounds,
        skipClosedUnmergedPullRequests: Bool
    ) throws -> [ReviewCandidate] {
        let context = currentContext()
        let descriptor = FetchDescriptor<PullRequestEntity>(
            predicate: #Predicate<PullRequestEntity> { record in
                record.repository == repository
            },
            sortBy: [SortDescriptor(\.number, order: .forward)]
        )
        let pullRequests = try context.fetch(descriptor)
        let watermarkDate = Timestamp.parse(watermarkUpdatedAt)

        return pullRequests.compactMap { record in
            guard let createdAtDate = Timestamp.parse(record.createdAt), dateBounds.includes(createdAtDate) else {
                return nil
            }

            if skipClosedUnmergedPullRequests, record.state.lowercased() == "closed", record.mergedAt == nil {
                return nil
            }

            if !backfill {
                if let watermarkDate {
                    if record.reviewScanState == "complete" {
                        guard
                            let updatedAtDate = Timestamp.parse(record.updatedAt),
                            updatedAtDate > watermarkDate
                        else {
                            return nil
                        }
                    }
                } else if record.reviewScanState == "complete" {
                    return nil
                }
            }

            return ReviewCandidate(number: record.number, updatedAt: record.updatedAt, createdAt: record.createdAt)
        }
    }

    func fetchAllPullRequests(repository: String) throws -> [PullRequestSnapshot] {
        let context = currentContext()
        let descriptor = FetchDescriptor<PullRequestEntity>(
            predicate: #Predicate<PullRequestEntity> { record in
                record.repository == repository
            },
            sortBy: [SortDescriptor(\.number, order: .forward)]
        )
        let records = try context.fetch(descriptor)

        return records.map { record in
            PullRequestSnapshot(
                number: record.number,
                createdAt: record.createdAt,
                closedAt: record.closedAt,
                mergedAt: record.mergedAt,
                state: record.state,
                updatedAt: record.updatedAt,
                firstApprovalAt: record.firstApprovalAt,
                firstApprovalReviewIdentifier: record.firstApprovalReviewIdentifier,
                firstApprovalActor: record.firstApprovalActor,
                reviewScanState: record.reviewScanState
            )
        }
    }

    func fetchAllReviews(repository: String) throws -> [PullRequestReviewSnapshot] {
        let context = currentContext()
        let descriptor = FetchDescriptor<PullRequestReviewEntity>(
            predicate: #Predicate<PullRequestReviewEntity> { record in
                record.repository == repository
            },
            sortBy: [
                SortDescriptor(\.pullRequestNumber, order: .forward),
                SortDescriptor(\.reviewIdentifier, order: .forward)
            ]
        )
        let records = try context.fetch(descriptor)

        return records.map { record in
            PullRequestReviewSnapshot(
                reviewIdentifier: record.reviewIdentifier,
                pullRequestNumber: record.pullRequestNumber,
                state: record.state,
                submittedAt: record.submittedAt,
                authorLogin: record.authorLogin
            )
        }
    }

    func listSyncStates(repository: String) throws -> [SyncStateRecord] {
        let context = currentContext()
        let descriptor = FetchDescriptor<SyncStateEntity>(
            predicate: #Predicate<SyncStateEntity> { record in
                record.repository == repository
            }
        )
        let records = try context.fetch(descriptor)

        return records
            .sorted { lhs, rhs in
                if lhs.phase == rhs.phase {
                    return lhs.mode < rhs.mode
                }
                return lhs.phase < rhs.phase
            }
            .map { record in
                SyncStateRecord(
                    phase: record.phase,
                    mode: record.mode,
                    cursor: record.cursor,
                    lastSuccessAt: record.lastSuccessAt,
                    watermarkUpdatedAt: record.watermarkUpdatedAt,
                    status: record.status,
                    error: record.error
                )
            }
    }

    func pullRequestCount(repository: String) throws -> Int {
        let context = currentContext()
        let descriptor = FetchDescriptor<PullRequestEntity>(
            predicate: #Predicate<PullRequestEntity> { record in
                record.repository == repository
            }
        )
        return try context.fetch(descriptor).count
    }

    func missingApprovalCount(repository: String) throws -> Int {
        let context = currentContext()
        let descriptor = FetchDescriptor<PullRequestEntity>(
            predicate: #Predicate<PullRequestEntity> { record in
                record.repository == repository
            }
        )

        return try context.fetch(descriptor).reduce(into: 0) { count, record in
            if record.firstApprovalAt == nil {
                count += 1
            }
        }
    }

    func maximumUpdatedAt(repository: String) throws -> String? {
        let context = currentContext()
        let descriptor = FetchDescriptor<PullRequestEntity>(
            predicate: #Predicate<PullRequestEntity> { record in
                record.repository == repository
            }
        )
        let maximumUpdatedDate = try context.fetch(descriptor)
            .compactMap { Timestamp.parse($0.updatedAt) }
            .max()

        return Timestamp.format(maximumUpdatedDate)
    }

    private func currentContext() -> ModelContext {
        transactionContext ?? ModelContext(modelContainer)
    }

    private func saveIfNeeded(_ context: ModelContext) throws {
        if transactionContext == nil, context.hasChanges {
            try context.save()
        }
    }

    private func fetchPullRequest(identifier: String, context: ModelContext) throws -> PullRequestEntity? {
        let descriptor = FetchDescriptor<PullRequestEntity>(
            predicate: #Predicate<PullRequestEntity> { record in
                record.identifier == identifier
            }
        )
        return try context.fetch(descriptor).first
    }

    private func fetchReview(identifier: String, context: ModelContext) throws -> PullRequestReviewEntity? {
        let descriptor = FetchDescriptor<PullRequestReviewEntity>(
            predicate: #Predicate<PullRequestReviewEntity> { record in
                record.identifier == identifier
            }
        )
        return try context.fetch(descriptor).first
    }

    private func fetchSyncState(identifier: String, context: ModelContext) throws -> SyncStateEntity? {
        let descriptor = FetchDescriptor<SyncStateEntity>(
            predicate: #Predicate<SyncStateEntity> { record in
                record.identifier == identifier
            }
        )
        return try context.fetch(descriptor).first
    }

    private func pullRequestIdentifier(repository: String, number: Int) -> String {
        "\(repository)#\(number)"
    }

    private func reviewIdentifier(repository: String, reviewIdentifier: Int64) -> String {
        "\(repository)#\(reviewIdentifier)"
    }

    private func syncStateIdentifier(repository: String, phase: String, mode: String) -> String {
        "\(repository)#\(phase)#\(mode)"
    }
}

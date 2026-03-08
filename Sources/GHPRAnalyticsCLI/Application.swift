import Foundation

final class Application {
    private let gitHubClient: GitHubClient
    private let fileManager: FileManager

    init(gitHubClient: GitHubClient = GitHubClient(), fileManager: FileManager = .default) {
        self.gitHubClient = gitHubClient
        self.fileManager = fileManager
    }

    func initialize(repositoryOverride: String?, databasePathOverride: String?) throws {
        let repository = try resolveRepository(explicitRepository: repositoryOverride)
        let storePath = try resolveStorePath(explicitDatabasePath: databasePathOverride, repository: repository)

        let database = try openStore(repository: repository, explicitDatabasePath: databasePathOverride)
        try database.migrate()

        fputs("Initialized analytics store for \(repository) at \(storePath)\n", stderr)
    }

    func setupWorkspace(
        repository: String,
        workspaceRootOverride: String?,
        databasePathOverride: String?
    ) throws {
        guard repository.contains("/") else {
            throw AnalyticsError.message("Repository must be in owner/name format. Received: \(repository)")
        }

        let workspaceRoot = workspaceRootOverride ?? ".gh-pr-analytics/workspaces"
        let safeRepositoryIdentifier = repository.replacingOccurrences(of: "/", with: "__")
        let workspacePath = "\(workspaceRoot)/\(safeRepositoryIdentifier)"
        let repositoryPath = "\(workspacePath)/repository"
        let storePath = databasePathOverride ?? "\(workspacePath)/analytics.swiftdata"

        if !fileManager.fileExists(atPath: workspacePath) {
            try fileManager.createDirectory(
                at: URL(fileURLWithPath: workspacePath),
                withIntermediateDirectories: true
            )
        }

        if fileManager.fileExists(atPath: repositoryPath) {
            fputs("Repository directory already exists at \(repositoryPath); skipping clone.\n", stderr)
        } else {
            try gitHubClient.cloneRepository(repository: repository, destinationPath: repositoryPath)
            fputs("Cloned \(repository) into \(repositoryPath)\n", stderr)
        }

        let store = try openStore(repository: repository, explicitDatabasePath: storePath)
        try store.migrate()

        fputs("Initialized analytics store at \(storePath)\n", stderr)
        print("workspace_path=\(workspacePath)")
        print("repository_path=\(repositoryPath)")
        print("database_path=\(storePath)")
    }

    func synchronize(
        repositoryOverride: String?,
        databasePathOverride: String?,
        phaseValue: String,
        backfill: Bool,
        resume: Bool,
        skipClosedUnmergedPullRequests: Bool,
        fromDate: String?,
        toDate: String?,
        batchSize: Int,
        maximumPages: Int?
    ) throws {
        let repository = try resolveRepository(explicitRepository: repositoryOverride)
        let phase = parsePhase(value: phaseValue)
        let validatedBatchSize = try parseBatchSize(value: batchSize)
        let validatedMaximumPages = try parseMaximumPages(value: maximumPages)
        let dateBounds = try parseDateBounds(fromDate: fromDate, toDate: toDate)

        let database = try openStore(repository: repository, explicitDatabasePath: databasePathOverride)
        try database.migrate()

        let syncEngine = SyncEngine(database: database, gitHubClient: gitHubClient)
        let syncConfiguration = SyncConfiguration(
            repository: repository,
            phase: phase,
            backfill: backfill,
            resume: resume,
            skipClosedUnmergedPullRequests: skipClosedUnmergedPullRequests,
            dateBounds: dateBounds,
            batchSize: validatedBatchSize,
            maximumPages: validatedMaximumPages
        )

        try syncEngine.synchronize(syncConfiguration)
    }

    func reportTrends(
        repositoryOverride: String?,
        databasePathOverride: String?,
        granularityValue: String,
        formatValue: String,
        fromDate: String?,
        toDate: String?
    ) throws {
        let repository = try resolveRepository(explicitRepository: repositoryOverride)
        let granularity = parseGranularity(value: granularityValue)
        let format = parseOutputFormat(value: formatValue)
        let dateBounds = try parseDateBounds(fromDate: fromDate, toDate: toDate)

        let database = try openStore(repository: repository, explicitDatabasePath: databasePathOverride)
        let reportingService = ReportingService(database: database)
        try reportingService.reportTrends(
            repository: repository,
            granularity: granularity,
            format: format,
            dateBounds: dateBounds
        )
    }

    func reportCharts(
        repositoryOverride: String?,
        databasePathOverride: String?,
        granularityValue: String,
        fromDate: String?,
        toDate: String?,
        outputPath: String,
        width: Int,
        height: Int
    ) throws {
        let repository = try resolveRepository(explicitRepository: repositoryOverride)
        let granularity = parseGranularity(value: granularityValue)
        let dateBounds = try parseDateBounds(fromDate: fromDate, toDate: toDate)
        let validatedWidth = try parseChartImageDimension(value: width, argumentName: "width")
        let validatedHeight = try parseChartImageDimension(value: height, argumentName: "height")

        let database = try openStore(repository: repository, explicitDatabasePath: databasePathOverride)
        let reportingService = ReportingService(database: database)

        try reportingService.renderTrendCharts(
            repository: repository,
            granularity: granularity,
            dateBounds: dateBounds,
            outputPath: outputPath,
            imageSize: CGSize(width: validatedWidth, height: validatedHeight)
        )
    }

    func exportPullRequests(
        repositoryOverride: String?,
        databasePathOverride: String?,
        formatValue: String
    ) throws {
        let repository = try resolveRepository(explicitRepository: repositoryOverride)
        let format = parseOutputFormat(value: formatValue)

        let database = try openStore(repository: repository, explicitDatabasePath: databasePathOverride)
        let reportingService = ReportingService(database: database)
        try reportingService.exportPullRequests(repository: repository, format: format)
    }

    func status(repositoryOverride: String?, databasePathOverride: String?) throws {
        let repository = try resolveRepository(explicitRepository: repositoryOverride)
        let storePath = try resolveStorePath(explicitDatabasePath: databasePathOverride, repository: repository)

        let database = try openStore(repository: repository, explicitDatabasePath: databasePathOverride)
        let pullRequestCount = try database.pullRequestCount(repository: repository)
        let missingApprovalCount = try database.missingApprovalCount(repository: repository)
        let syncStates = try database.listSyncStates(repository: repository)

        print("repository=\(repository)")
        print("database_path=\(storePath)")
        print("pull_requests=\(pullRequestCount)")
        print("missing_first_approval=\(missingApprovalCount)")

        if syncStates.isEmpty {
            print("sync_state=none")
        } else {
            for state in syncStates {
                print(
                    "sync_state phase=\(state.phase) mode=\(state.mode) status=\(state.status ?? "unknown") cursor=\(state.cursor ?? "") last_success_at=\(state.lastSuccessAt ?? "") watermark_updated_at=\(state.watermarkUpdatedAt ?? "") error=\(state.error ?? "")"
                )
            }
        }
    }

    private func resolveRepository(explicitRepository: String?) throws -> String {
        if let explicitRepository {
            guard explicitRepository.contains("/") else {
                throw AnalyticsError.message("Repository must be in owner/name format. Received: \(explicitRepository)")
            }

            return explicitRepository
        }

        return try gitHubClient.resolveRepositoryFromCurrentDirectory()
    }

    private func resolveStorePath(explicitDatabasePath: String?, repository: String) throws -> String {
        if let explicitDatabasePath {
            return swiftDataPath(from: explicitDatabasePath)
        }

        let path = defaultWorkspaceStorePath(repository: repository)

        guard !path.isEmpty else {
            throw AnalyticsError.message("Unable to determine default analytics store path")
        }

        return path
    }

    private func resolveLegacySQLitePath(explicitDatabasePath: String?, repository: String) -> String? {
        if let explicitDatabasePath, explicitDatabasePath.lowercased().hasSuffix(".sqlite") {
            return explicitDatabasePath
        }

        guard explicitDatabasePath == nil else {
            return nil
        }

        let safeRepositoryIdentifier = repository.replacingOccurrences(of: "/", with: "__")
        return ".gh-pr-analytics/\(safeRepositoryIdentifier).sqlite"
    }

    private func defaultWorkspaceStorePath(repository: String) -> String {
        let safeRepositoryIdentifier = repository.replacingOccurrences(of: "/", with: "__")
        return ".gh-pr-analytics/workspaces/\(safeRepositoryIdentifier)/analytics.swiftdata"
    }

    private func swiftDataPath(from providedPath: String) -> String {
        let lowercasedPath = providedPath.lowercased()
        if lowercasedPath.hasSuffix(".sqlite") {
            let extensionLength = ".sqlite".count
            let prefix = providedPath.dropLast(extensionLength)
            return "\(prefix).swiftdata"
        }

        return providedPath
    }

    private func openStore(repository: String, explicitDatabasePath: String?) throws -> AnalyticsStore {
        let storePath = try resolveStorePath(explicitDatabasePath: explicitDatabasePath, repository: repository)
        let store = try SwiftDataStore(path: storePath)
        try store.migrate()

        let legacySQLitePath = resolveLegacySQLitePath(explicitDatabasePath: explicitDatabasePath, repository: repository)
        try importLegacySQLiteDataIfNeeded(
            repository: repository,
            store: store,
            storePath: storePath,
            legacySQLitePath: legacySQLitePath
        )

        return store
    }

    private func importLegacySQLiteDataIfNeeded(
        repository: String,
        store: AnalyticsStore,
        storePath: String,
        legacySQLitePath: String?
    ) throws {
        guard let legacySQLitePath else {
            return
        }

        guard fileManager.fileExists(atPath: legacySQLitePath) else {
            return
        }

        let storeAlreadyHasData = try store.pullRequestCount(repository: repository) > 0
        guard !storeAlreadyHasData else {
            return
        }

        let legacyDatabase = try SQLiteDatabase(path: legacySQLitePath)
        let pullRequests = try legacyDatabase.fetchAllPullRequests(repository: repository)
        let reviews = try legacyDatabase.fetchAllReviews(repository: repository)
        let syncStates = try legacyDatabase.listSyncStates(repository: repository)

        try store.inTransaction {
            for pullRequest in pullRequests {
                let metadata = PullRequestMetadata(
                    number: pullRequest.number,
                    createdAt: pullRequest.createdAt,
                    closedAt: pullRequest.closedAt,
                    mergedAt: pullRequest.mergedAt,
                    state: pullRequest.state,
                    updatedAt: pullRequest.updatedAt
                )

                try store.upsertPullRequest(
                    repository: repository,
                    metadata: metadata,
                    reviewScanState: pullRequest.reviewScanState
                )

                if let firstApprovalAt = pullRequest.firstApprovalAt {
                    let approvalEvent = ApprovalEvent(
                        submittedAt: firstApprovalAt,
                        reviewIdentifier: pullRequest.firstApprovalReviewIdentifier,
                        actor: pullRequest.firstApprovalActor
                    )
                    try store.setFirstApproval(
                        repository: repository,
                        pullRequestNumber: pullRequest.number,
                        approvalEvent: approvalEvent
                    )
                }
            }

            for review in reviews {
                let reviewAuthor = review.authorLogin.map { PullRequestReviewAuthor(login: $0) }
                let pullRequestReview = PullRequestReview(
                    reviewIdentifier: review.reviewIdentifier,
                    state: review.state,
                    submittedAt: review.submittedAt,
                    user: reviewAuthor
                )

                try store.upsertReview(
                    repository: repository,
                    pullRequestNumber: review.pullRequestNumber,
                    review: pullRequestReview
                )
            }

            for syncState in syncStates {
                try store.upsertSyncState(
                    repository: repository,
                    phase: syncState.phase,
                    mode: syncState.mode,
                    cursor: syncState.cursor,
                    lastSuccessAt: syncState.lastSuccessAt,
                    watermarkUpdatedAt: syncState.watermarkUpdatedAt,
                    status: syncState.status,
                    error: syncState.error
                )
            }
        }

        fputs(
            "Imported legacy SQLite analytics data from \(legacySQLitePath) into \(storePath)\n",
            stderr
        )
    }

    private func parsePhase(value: String) -> SyncPhaseOption {
        SyncPhaseOption(rawValue: value) ?? .all
    }

    private func parseGranularity(value: String) -> TrendGranularity {
        TrendGranularity(rawValue: value) ?? .week
    }

    private func parseOutputFormat(value: String) -> OutputFormat {
        OutputFormat(rawValue: value) ?? .csv
    }

    private func parseBatchSize(value: Int) throws -> Int {
        guard (1 ... 100).contains(value) else {
            throw AnalyticsError.message("batch-size must be between 1 and 100")
        }

        return value
    }

    private func parseMaximumPages(value: Int?) throws -> Int? {
        guard let value else {
            return nil
        }

        guard value > 0 else {
            throw AnalyticsError.message("max-pages must be a positive integer")
        }

        return value
    }

    private func parseChartImageDimension(value: Int, argumentName: String) throws -> Double {
        guard value > 0 else {
            throw AnalyticsError.message("\(argumentName) must be a positive integer")
        }

        return Double(value)
    }

    private func parseDateBounds(fromDate: String?, toDate: String?) throws -> DateBounds {
        let from = Timestamp.parseDayBoundaryStart(fromDate)
        let to = Timestamp.parseDayBoundaryEnd(toDate)

        if fromDate != nil, from == nil {
            throw AnalyticsError.message("from-date must use YYYY-MM-DD format")
        }

        if toDate != nil, to == nil {
            throw AnalyticsError.message("to-date must use YYYY-MM-DD format")
        }

        if let from, let to, from > to {
            throw AnalyticsError.message("from-date cannot be after to-date")
        }

        return DateBounds(from: from, to: to)
    }
}

import Foundation

struct SyncConfiguration {
    let repository: String
    let phase: SyncPhaseOption
    let backfill: Bool
    let resume: Bool
    let skipClosedUnmergedPullRequests: Bool
    let dateBounds: DateBounds
    let batchSize: Int
    let maximumPages: Int?
}

final class SyncEngine {
    private let database: AnalyticsStore
    private let gitHubClient: GitHubClient

    init(database: AnalyticsStore, gitHubClient: GitHubClient) {
        self.database = database
        self.gitHubClient = gitHubClient
    }

    func synchronize(_ configuration: SyncConfiguration) throws {
        fputs(
            "Synchronization started for \(configuration.repository) with minimum request delay \(gitHubClient.minimumRequestDelayMilliseconds) milliseconds, request timeout \(gitHubClient.requestTimeoutMilliseconds) milliseconds, and maximum attempts \(gitHubClient.maximumAttempts).\n",
            stderr
        )

        switch configuration.phase {
        case .metadata:
            try synchronizeMetadata(configuration)
        case .reviews:
            try synchronizeReviews(configuration)
        case .all:
            try synchronizeMetadata(configuration)
            try synchronizeReviews(configuration)
        }
    }

    private func synchronizeMetadata(_ configuration: SyncConfiguration) throws {
        let mode = configuration.backfill ? "backfill" : "incremental"
        let previousState = try database.fetchSyncState(repository: configuration.repository, phase: "metadata", mode: mode)
        let existingWatermarkDate = Timestamp.parse(previousState?.watermarkUpdatedAt)
        fputs(
            "Metadata synchronization running in \(mode) mode for \(configuration.repository).\n",
            stderr
        )

        var page = 1
        if configuration.resume,
           let cursor = previousState?.cursor,
           let completedPage = Int(cursor)
        {
            page = completedPage + 1
            fputs("Resuming metadata synchronization from page \(page).\n", stderr)
        }

        var pagesProcessed = 0
        var pullRequestsStored = 0
        var newestUpdatedAtDate = existingWatermarkDate
        var reachedExistingWatermark = false
        var reachedUpperDateBound = false

        let sort = configuration.backfill ? "created" : "updated"
        let direction = configuration.backfill ? "asc" : "desc"

        while true {
            if let maximumPages = configuration.maximumPages, pagesProcessed >= maximumPages {
                fputs("Reached metadata maximum page limit of \(maximumPages).\n", stderr)
                break
            }

            fputs("Fetching metadata page \(page)...\n", stderr)

            let metadataPage = try gitHubClient.fetchPullRequestMetadata(
                repository: configuration.repository,
                sort: sort,
                direction: direction,
                page: page,
                perPage: configuration.batchSize
            )

            if metadataPage.isEmpty {
                fputs("Metadata page \(page) returned no pull requests.\n", stderr)
                break
            }

            try database.inTransaction {
                for metadata in metadataPage {
                    guard
                        let createdAtDate = Timestamp.parse(metadata.createdAt),
                        let updatedAtDate = Timestamp.parse(metadata.updatedAt)
                    else {
                        continue
                    }

                    if let upperBound = configuration.dateBounds.to,
                       configuration.backfill,
                       createdAtDate > upperBound
                    {
                        reachedUpperDateBound = true
                        break
                    }

                    if !configuration.dateBounds.includes(createdAtDate) {
                        continue
                    }

                    if !configuration.backfill,
                       let existingWatermarkDate,
                       updatedAtDate <= existingWatermarkDate
                    {
                        reachedExistingWatermark = true
                        continue
                    }

                    try database.upsertPullRequest(repository: configuration.repository, metadata: metadata)
                    pullRequestsStored += 1

                    if let latest = newestUpdatedAtDate {
                        newestUpdatedAtDate = max(latest, updatedAtDate)
                    } else {
                        newestUpdatedAtDate = updatedAtDate
                    }
                }
            }

            pagesProcessed += 1
            fputs(
                "Processed metadata page \(page): received \(metadataPage.count) pull requests, stored \(pullRequestsStored) total.\n",
                stderr
            )

            try database.upsertSyncState(
                repository: configuration.repository,
                phase: "metadata",
                mode: mode,
                cursor: String(page),
                lastSuccessAt: previousState?.lastSuccessAt,
                watermarkUpdatedAt: previousState?.watermarkUpdatedAt,
                status: "running",
                error: nil
            )

            if reachedExistingWatermark || reachedUpperDateBound {
                if reachedExistingWatermark {
                    fputs("Reached existing watermark during metadata synchronization.\n", stderr)
                }
                if reachedUpperDateBound {
                    fputs("Reached upper date bound during metadata synchronization.\n", stderr)
                }
                break
            }

            page += 1
        }

        let finalWatermark = Timestamp.format(newestUpdatedAtDate) ?? previousState?.watermarkUpdatedAt

        try database.upsertSyncState(
            repository: configuration.repository,
            phase: "metadata",
            mode: mode,
            cursor: nil,
            lastSuccessAt: Timestamp.format(Date()),
            watermarkUpdatedAt: finalWatermark,
            status: "success",
            error: nil
        )

        fputs(
            "Metadata synchronization complete for \(configuration.repository). Stored \(pullRequestsStored) pull requests over \(pagesProcessed) pages.\n",
            stderr
        )
    }

    private func synchronizeReviews(_ configuration: SyncConfiguration) throws {
        let mode = configuration.backfill ? "backfill" : "incremental"
        let previousState = try database.fetchSyncState(repository: configuration.repository, phase: "reviews", mode: mode)

        let candidatePullRequests = try database.fetchReviewCandidates(
            repository: configuration.repository,
            backfill: configuration.backfill,
            watermarkUpdatedAt: previousState?.watermarkUpdatedAt,
            dateBounds: configuration.dateBounds,
            skipClosedUnmergedPullRequests: configuration.skipClosedUnmergedPullRequests
        )

        var startIndex = 0
        if configuration.resume,
           let cursor = previousState?.cursor,
           let completedIndex = Int(cursor)
        {
            startIndex = min(completedIndex + 1, candidatePullRequests.count)
            fputs("Resuming review enrichment from pull request index \(startIndex + 1).\n", stderr)
        }

        let remainingPullRequestCount = max(0, candidatePullRequests.count - startIndex)
        fputs(
            "Review enrichment running in \(mode) mode for \(configuration.repository). Pending pull requests: \(remainingPullRequestCount).\n",
            stderr
        )

        var candidatesProcessed = 0

        for index in startIndex ..< candidatePullRequests.count {
            let candidate = candidatePullRequests[index]
            var reviewPage = 1
            var earliestApprovalEvent: ApprovalEvent?
            let displayIndex = index + 1
            fputs(
                "Processing pull request #\(candidate.number) (\(displayIndex)/\(candidatePullRequests.count)).\n",
                stderr
            )

            while true {
                if let maximumPages = configuration.maximumPages,
                   reviewPage > maximumPages
                {
                    fputs(
                        "Reached review page limit \(maximumPages) for pull request #\(candidate.number).\n",
                        stderr
                    )
                    break
                }

                fputs(
                    "Fetching reviews for pull request #\(candidate.number), page \(reviewPage)...\n",
                    stderr
                )

                let reviews = try gitHubClient.fetchPullRequestReviews(
                    repository: configuration.repository,
                    pullRequestNumber: candidate.number,
                    page: reviewPage,
                    perPage: configuration.batchSize
                )

                if reviews.isEmpty {
                    fputs(
                        "Review page \(reviewPage) for pull request #\(candidate.number) returned no records.\n",
                        stderr
                    )
                    break
                }

                try database.inTransaction {
                    for review in reviews {
                        try database.upsertReview(
                            repository: configuration.repository,
                            pullRequestNumber: candidate.number,
                            review: review
                        )

                        guard
                            review.state.uppercased() == "APPROVED",
                            let submittedAt = review.submittedAt,
                            let submittedAtDate = Timestamp.parse(submittedAt)
                        else {
                            continue
                        }

                        if let existingApproval = earliestApprovalEvent,
                           let existingDate = Timestamp.parse(existingApproval.submittedAt),
                           submittedAtDate >= existingDate
                        {
                            continue
                        }

                        earliestApprovalEvent = ApprovalEvent(
                            submittedAt: submittedAt,
                            reviewIdentifier: review.reviewIdentifier,
                            actor: review.user?.login
                        )
                    }
                }

                if reviews.count < configuration.batchSize {
                    fputs(
                        "Review page \(reviewPage) for pull request #\(candidate.number) was not full; ending pagination.\n",
                        stderr
                    )
                    break
                }

                reviewPage += 1
            }

            try database.setFirstApproval(
                repository: configuration.repository,
                pullRequestNumber: candidate.number,
                approvalEvent: earliestApprovalEvent
            )

            candidatesProcessed += 1
            if let earliestApprovalEvent {
                fputs(
                    "Recorded first approval for pull request #\(candidate.number) at \(earliestApprovalEvent.submittedAt).\n",
                    stderr
                )
            } else {
                fputs(
                    "No approval found for pull request #\(candidate.number).\n",
                    stderr
                )
            }

            try database.upsertSyncState(
                repository: configuration.repository,
                phase: "reviews",
                mode: mode,
                cursor: String(index),
                lastSuccessAt: previousState?.lastSuccessAt,
                watermarkUpdatedAt: previousState?.watermarkUpdatedAt,
                status: "running",
                error: nil
            )
        }

        let finalWatermark = try database.maximumUpdatedAt(repository: configuration.repository) ?? previousState?.watermarkUpdatedAt

        try database.upsertSyncState(
            repository: configuration.repository,
            phase: "reviews",
            mode: mode,
            cursor: nil,
            lastSuccessAt: Timestamp.format(Date()),
            watermarkUpdatedAt: finalWatermark,
            status: "success",
            error: nil
        )

        fputs(
            "Review enrichment complete for \(configuration.repository). Processed \(candidatesProcessed) pull requests.\n",
            stderr
        )
    }
}

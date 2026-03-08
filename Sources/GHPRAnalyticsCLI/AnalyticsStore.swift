import Foundation

protocol AnalyticsStore {
    func migrate() throws
    func inTransaction(_ body: () throws -> Void) throws

    func upsertPullRequest(
        repository: String,
        metadata: PullRequestMetadata,
        reviewScanState: String
    ) throws
    func upsertReview(repository: String, pullRequestNumber: Int, review: PullRequestReview) throws
    func setFirstApproval(repository: String, pullRequestNumber: Int, approvalEvent: ApprovalEvent?) throws

    func fetchSyncState(repository: String, phase: String, mode: String) throws -> SyncStateRecord?
    func upsertSyncState(
        repository: String,
        phase: String,
        mode: String,
        cursor: String?,
        lastSuccessAt: String?,
        watermarkUpdatedAt: String?,
        status: String?,
        error: String?
    ) throws

    func fetchReviewCandidates(
        repository: String,
        backfill: Bool,
        watermarkUpdatedAt: String?,
        dateBounds: DateBounds,
        skipClosedUnmergedPullRequests: Bool
    ) throws -> [ReviewCandidate]

    func fetchAllPullRequests(repository: String) throws -> [PullRequestSnapshot]
    func fetchAllReviews(repository: String) throws -> [PullRequestReviewSnapshot]
    func listSyncStates(repository: String) throws -> [SyncStateRecord]
    func pullRequestCount(repository: String) throws -> Int
    func missingApprovalCount(repository: String) throws -> Int
    func maximumUpdatedAt(repository: String) throws -> String?
}

extension AnalyticsStore {
    func upsertPullRequest(
        repository: String,
        metadata: PullRequestMetadata
    ) throws {
        try upsertPullRequest(repository: repository, metadata: metadata, reviewScanState: "pending")
    }
}

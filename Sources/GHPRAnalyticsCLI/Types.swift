import Foundation

enum AnalyticsError: Error, CustomStringConvertible {
    case message(String)

    var description: String {
        switch self {
        case let .message(value):
            return value
        }
    }
}

enum SyncPhaseOption: String {
    case metadata
    case reviews
    case all
}

enum TrendGranularity: String {
    case week
    case month
}

enum OutputFormat: String {
    case csv
    case json
}

struct DateBounds {
    let from: Date?
    let to: Date?

    func includes(_ value: Date) -> Bool {
        if let from, value < from {
            return false
        }

        if let to, value > to {
            return false
        }

        return true
    }
}

enum Timestamp {
    private static func parserWithFractionalSeconds() -> ISO8601DateFormatter {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter
    }

    private static func parserWithoutFractionalSeconds() -> ISO8601DateFormatter {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        return formatter
    }

    private static func dayFormatter() -> DateFormatter {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd"
        return formatter
    }

    private static func csvFormatter() -> DateFormatter {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"
        return formatter
    }

    static func parse(_ value: String?) -> Date? {
        guard let value else {
            return nil
        }

        if let parsed = parserWithFractionalSeconds().date(from: value) {
            return parsed
        }

        return parserWithoutFractionalSeconds().date(from: value)
    }

    static func parseDayBoundaryStart(_ value: String?) -> Date? {
        guard let value else {
            return nil
        }

        return dayFormatter().date(from: value)
    }

    static func parseDayBoundaryEnd(_ value: String?) -> Date? {
        guard let start = parseDayBoundaryStart(value) else {
            return nil
        }

        return start.addingTimeInterval(86_399)
    }

    static func format(_ value: Date?) -> String? {
        guard let value else {
            return nil
        }

        return csvFormatter().string(from: value)
    }
}

struct PullRequestMetadata: Decodable {
    let number: Int
    let createdAt: String
    let closedAt: String?
    let mergedAt: String?
    let state: String
    let updatedAt: String

    enum CodingKeys: String, CodingKey {
        case number
        case createdAt = "created_at"
        case closedAt = "closed_at"
        case mergedAt = "merged_at"
        case state
        case updatedAt = "updated_at"
    }
}

struct PullRequestReviewAuthor: Decodable {
    let login: String?
}

struct PullRequestReview: Decodable {
    let reviewIdentifier: Int64
    let state: String
    let submittedAt: String?
    let user: PullRequestReviewAuthor?

    enum CodingKeys: String, CodingKey {
        case reviewIdentifier = "id"
        case state
        case submittedAt = "submitted_at"
        case user
    }
}

struct PullRequestSnapshot {
    let number: Int
    let createdAt: String
    let closedAt: String?
    let mergedAt: String?
    let state: String
    let updatedAt: String
    let firstApprovalAt: String?
    let firstApprovalReviewIdentifier: Int64?
    let firstApprovalActor: String?
    let reviewScanState: String
}

struct PullRequestReviewSnapshot {
    let reviewIdentifier: Int64
    let pullRequestNumber: Int
    let state: String
    let submittedAt: String?
    let authorLogin: String?
}

struct ReviewCandidate {
    let number: Int
    let updatedAt: String
    let createdAt: String
}

struct SyncStateRecord {
    let phase: String
    let mode: String
    let cursor: String?
    let lastSuccessAt: String?
    let watermarkUpdatedAt: String?
    let status: String?
    let error: String?
}

struct ApprovalEvent {
    let submittedAt: String
    let reviewIdentifier: Int64?
    let actor: String?
}

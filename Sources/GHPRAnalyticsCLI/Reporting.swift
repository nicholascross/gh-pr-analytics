import Foundation

struct TrendRow: Encodable {
    let periodStart: String
    let periodGranularity: String
    let pullRequestsOpened: Int
    let pullRequestsMerged: Int
    let timeToMergeP50Hours: Double?
    let timeToMergeP90Hours: Double?
    let timeToFirstApprovalP50Hours: Double?
    let timeToFirstApprovalP90Hours: Double?
    let mergeSampleSize: Int
    let approvalSampleSize: Int
}

struct FunnelRow {
    let periodStart: Date
    let pullRequestsOpened: Int
    let pullRequestsFirstApproved: Int
    let pullRequestsMerged: Int
    let pullRequestsClosedWithoutMerge: Int
}

struct CycleDistributionRow {
    let periodStart: Date
    let mergeTimeP10Hours: Double?
    let mergeTimeP50Hours: Double?
    let mergeTimeP90Hours: Double?
    let mergeSampleSize: Int
}

struct LagCorrelationPoint {
    let pullRequestNumber: Int
    let approvalLagHours: Double
    let mergeLagHours: Double
}

struct OpenPullRequestAgeBucket {
    let label: String
    let pullRequestCount: Int
}

final class ReportingService {
    private let database: AnalyticsStore
    private let trendChartRenderer: TrendChartRenderer
    private let insightsChartRenderer: InsightsChartRenderer

    init(
        database: AnalyticsStore,
        trendChartRenderer: TrendChartRenderer = TrendChartRenderer(),
        insightsChartRenderer: InsightsChartRenderer = InsightsChartRenderer()
    ) {
        self.database = database
        self.trendChartRenderer = trendChartRenderer
        self.insightsChartRenderer = insightsChartRenderer
    }

    func exportPullRequests(repository: String, format: OutputFormat) throws {
        let pullRequests = try database.fetchAllPullRequests(repository: repository)

        switch format {
        case .json:
            let jsonObjects: [[String: Any?]] = pullRequests.map { pullRequest in
                let timeToMergeHours = Self.timeDifferenceInHours(startTimestamp: pullRequest.createdAt, endTimestamp: pullRequest.mergedAt)
                let timeToFirstApprovalHours = Self.timeDifferenceInHours(startTimestamp: pullRequest.createdAt, endTimestamp: pullRequest.firstApprovalAt)

                return [
                    "number": pullRequest.number,
                    "created_at": pullRequest.createdAt,
                    "closed_at": pullRequest.closedAt,
                    "merged_at": pullRequest.mergedAt,
                    "state": pullRequest.state,
                    "first_approval_at": pullRequest.firstApprovalAt,
                    "time_to_merge_hours": timeToMergeHours,
                    "time_to_first_approval_hours": timeToFirstApprovalHours
                ]
            }

            let outputData = try JSONSerialization.data(withJSONObject: jsonObjects, options: [.prettyPrinted, .sortedKeys])
            if let outputString = String(data: outputData, encoding: .utf8) {
                print(outputString)
            }

        case .csv:
            print("number,created_at,closed_at,merged_at,state,first_approval_at,time_to_merge_hours,time_to_first_approval_hours")
            for pullRequest in pullRequests {
                let timeToMergeHours = Self.timeDifferenceInHours(startTimestamp: pullRequest.createdAt, endTimestamp: pullRequest.mergedAt)
                let timeToFirstApprovalHours = Self.timeDifferenceInHours(startTimestamp: pullRequest.createdAt, endTimestamp: pullRequest.firstApprovalAt)

                let row = [
                    String(pullRequest.number),
                    csvEscape(pullRequest.createdAt),
                    csvEscape(pullRequest.closedAt),
                    csvEscape(pullRequest.mergedAt),
                    csvEscape(pullRequest.state),
                    csvEscape(pullRequest.firstApprovalAt),
                    csvEscape(timeToMergeHours.map { Self.formatDecimal($0) }),
                    csvEscape(timeToFirstApprovalHours.map { Self.formatDecimal($0) })
                ].joined(separator: ",")

                print(row)
            }
        }
    }

    func reportTrends(
        repository: String,
        granularity: TrendGranularity,
        format: OutputFormat,
        dateBounds: DateBounds
    ) throws {
        let pullRequests = try database.fetchAllPullRequests(repository: repository)
        let trendRows = calculateTrendRows(
            pullRequests: pullRequests,
            granularity: granularity,
            dateBounds: dateBounds
        )

        switch format {
        case .json:
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            let outputData = try encoder.encode(trendRows)
            if let outputString = String(data: outputData, encoding: .utf8) {
                print(outputString)
            }

        case .csv:
            print(
                "period_start,period_granularity,pull_requests_opened,pull_requests_merged,time_to_merge_p50_hours,time_to_merge_p90_hours,time_to_first_approval_p50_hours,time_to_first_approval_p90_hours,merge_sample_size,approval_sample_size"
            )

            for row in trendRows {
                let csvRow = [
                    csvEscape(row.periodStart),
                    csvEscape(row.periodGranularity),
                    String(row.pullRequestsOpened),
                    String(row.pullRequestsMerged),
                    csvEscape(row.timeToMergeP50Hours.map { Self.formatDecimal($0) }),
                    csvEscape(row.timeToMergeP90Hours.map { Self.formatDecimal($0) }),
                    csvEscape(row.timeToFirstApprovalP50Hours.map { Self.formatDecimal($0) }),
                    csvEscape(row.timeToFirstApprovalP90Hours.map { Self.formatDecimal($0) }),
                    String(row.mergeSampleSize),
                    String(row.approvalSampleSize)
                ].joined(separator: ",")

                print(csvRow)
            }
        }
    }

    func renderCharts(
        repository: String,
        granularity: TrendGranularity,
        chartStyle: ChartStyle,
        dateBounds: DateBounds,
        outputPath: String,
        imageSize: CGSize
    ) throws {
        let pullRequests = try database.fetchAllPullRequests(repository: repository)

        switch chartStyle {
        case .trend:
            let trendRows = calculateTrendRows(
                pullRequests: pullRequests,
                granularity: granularity,
                dateBounds: dateBounds
            )

            try trendChartRenderer.render(
                repository: repository,
                granularity: granularity,
                trendRows: trendRows,
                imageSize: imageSize,
                outputPath: outputPath
            )
        case .insights:
            let funnelRows = calculateFunnelRows(
                pullRequests: pullRequests,
                granularity: granularity,
                dateBounds: dateBounds
            )
            let cycleDistributionRows = calculateCycleDistributionRows(
                pullRequests: pullRequests,
                granularity: granularity,
                dateBounds: dateBounds
            )
            let lagCorrelationPoints = calculateLagCorrelationPoints(
                pullRequests: pullRequests,
                dateBounds: dateBounds
            )
            let openPullRequestAgeBuckets = calculateOpenPullRequestAgeBuckets(
                pullRequests: pullRequests,
                dateBounds: dateBounds,
                now: Date()
            )

            try insightsChartRenderer.render(
                repository: repository,
                granularity: granularity,
                funnelRows: funnelRows,
                cycleDistributionRows: cycleDistributionRows,
                lagCorrelationPoints: lagCorrelationPoints,
                openPullRequestAgeBuckets: openPullRequestAgeBuckets,
                imageSize: imageSize,
                outputPath: outputPath
            )
        }

        fputs("Chart image written to \(outputPath)\n", stderr)
    }

    private func calculateFunnelRows(
        pullRequests: [PullRequestSnapshot],
        granularity: TrendGranularity,
        dateBounds: DateBounds
    ) -> [FunnelRow] {
        var buckets: [String: (periodStartDate: Date, opened: Int, firstApproved: Int, merged: Int, closedWithoutMerge: Int)] = [:]

        for pullRequest in pullRequests {
            guard let createdAtDate = Timestamp.parse(pullRequest.createdAt), dateBounds.includes(createdAtDate) else {
                continue
            }

            let periodStartDate = Self.periodStart(for: createdAtDate, granularity: granularity)
            let periodKey = Timestamp.format(periodStartDate) ?? pullRequest.createdAt
            var bucket = buckets[periodKey] ?? (periodStartDate, 0, 0, 0, 0)
            bucket.opened += 1

            if pullRequest.firstApprovalAt != nil {
                bucket.firstApproved += 1
            }

            if pullRequest.mergedAt != nil {
                bucket.merged += 1
            } else if pullRequest.state.lowercased() == "closed" {
                bucket.closedWithoutMerge += 1
            }

            buckets[periodKey] = bucket
        }

        return buckets
            .values
            .sorted { $0.periodStartDate < $1.periodStartDate }
            .map { bucket in
                FunnelRow(
                    periodStart: bucket.periodStartDate,
                    pullRequestsOpened: bucket.opened,
                    pullRequestsFirstApproved: bucket.firstApproved,
                    pullRequestsMerged: bucket.merged,
                    pullRequestsClosedWithoutMerge: bucket.closedWithoutMerge
                )
            }
    }

    private func calculateCycleDistributionRows(
        pullRequests: [PullRequestSnapshot],
        granularity: TrendGranularity,
        dateBounds: DateBounds
    ) -> [CycleDistributionRow] {
        var buckets: [String: (periodStartDate: Date, mergeHours: [Double])] = [:]

        for pullRequest in pullRequests {
            guard
                let mergedAt = pullRequest.mergedAt,
                let mergedAtDate = Timestamp.parse(mergedAt),
                dateBounds.includes(mergedAtDate),
                let mergeHours = Self.timeDifferenceInHours(startTimestamp: pullRequest.createdAt, endTimestamp: mergedAt)
            else {
                continue
            }

            let periodStartDate = Self.periodStart(for: mergedAtDate, granularity: granularity)
            let periodKey = Timestamp.format(periodStartDate) ?? mergedAt
            var bucket = buckets[periodKey] ?? (periodStartDate, [])
            bucket.mergeHours.append(mergeHours)
            buckets[periodKey] = bucket
        }

        return buckets
            .values
            .sorted { $0.periodStartDate < $1.periodStartDate }
            .map { bucket in
                CycleDistributionRow(
                    periodStart: bucket.periodStartDate,
                    mergeTimeP10Hours: Self.percentile(bucket.mergeHours, percentile: 0.10),
                    mergeTimeP50Hours: Self.percentile(bucket.mergeHours, percentile: 0.50),
                    mergeTimeP90Hours: Self.percentile(bucket.mergeHours, percentile: 0.90),
                    mergeSampleSize: bucket.mergeHours.count
                )
            }
    }

    private func calculateLagCorrelationPoints(
        pullRequests: [PullRequestSnapshot],
        dateBounds: DateBounds
    ) -> [LagCorrelationPoint] {
        pullRequests.compactMap { pullRequest in
            guard
                let createdAtDate = Timestamp.parse(pullRequest.createdAt),
                dateBounds.includes(createdAtDate),
                let approvalLagHours = Self.timeDifferenceInHours(
                    startTimestamp: pullRequest.createdAt,
                    endTimestamp: pullRequest.firstApprovalAt
                ),
                let mergeLagHours = Self.timeDifferenceInHours(
                    startTimestamp: pullRequest.createdAt,
                    endTimestamp: pullRequest.mergedAt
                )
            else {
                return nil
            }

            return LagCorrelationPoint(
                pullRequestNumber: pullRequest.number,
                approvalLagHours: approvalLagHours,
                mergeLagHours: mergeLagHours
            )
        }
    }

    private func calculateOpenPullRequestAgeBuckets(
        pullRequests: [PullRequestSnapshot],
        dateBounds: DateBounds,
        now: Date
    ) -> [OpenPullRequestAgeBucket] {
        var bucketCounts = [
            OpenPullRequestAgeBucket(label: "0-1 days", pullRequestCount: 0),
            OpenPullRequestAgeBucket(label: "1-3 days", pullRequestCount: 0),
            OpenPullRequestAgeBucket(label: "3-7 days", pullRequestCount: 0),
            OpenPullRequestAgeBucket(label: "7-14 days", pullRequestCount: 0),
            OpenPullRequestAgeBucket(label: "14-30 days", pullRequestCount: 0),
            OpenPullRequestAgeBucket(label: "30+ days", pullRequestCount: 0)
        ]

        for pullRequest in pullRequests {
            guard
                pullRequest.state.lowercased() == "open",
                pullRequest.mergedAt == nil,
                let createdAtDate = Timestamp.parse(pullRequest.createdAt),
                dateBounds.includes(createdAtDate)
            else {
                continue
            }

            let ageDays = max(0.0, now.timeIntervalSince(createdAtDate) / 86_400.0)

            switch ageDays {
            case 0..<1:
                bucketCounts[0] = OpenPullRequestAgeBucket(label: bucketCounts[0].label, pullRequestCount: bucketCounts[0].pullRequestCount + 1)
            case 1..<3:
                bucketCounts[1] = OpenPullRequestAgeBucket(label: bucketCounts[1].label, pullRequestCount: bucketCounts[1].pullRequestCount + 1)
            case 3..<7:
                bucketCounts[2] = OpenPullRequestAgeBucket(label: bucketCounts[2].label, pullRequestCount: bucketCounts[2].pullRequestCount + 1)
            case 7..<14:
                bucketCounts[3] = OpenPullRequestAgeBucket(label: bucketCounts[3].label, pullRequestCount: bucketCounts[3].pullRequestCount + 1)
            case 14..<30:
                bucketCounts[4] = OpenPullRequestAgeBucket(label: bucketCounts[4].label, pullRequestCount: bucketCounts[4].pullRequestCount + 1)
            default:
                bucketCounts[5] = OpenPullRequestAgeBucket(label: bucketCounts[5].label, pullRequestCount: bucketCounts[5].pullRequestCount + 1)
            }
        }

        return bucketCounts
    }

    private func calculateTrendRows(
        pullRequests: [PullRequestSnapshot],
        granularity: TrendGranularity,
        dateBounds: DateBounds
    ) -> [TrendRow] {
        var buckets: [String: (periodStartDate: Date, opened: Int, merged: Int, mergeHours: [Double], approvalHours: [Double])] = [:]

        for pullRequest in pullRequests {
            guard let createdAtDate = Timestamp.parse(pullRequest.createdAt) else {
                continue
            }

            if dateBounds.includes(createdAtDate) {
                let periodStartDate = Self.periodStart(for: createdAtDate, granularity: granularity)
                let periodKey = Timestamp.format(periodStartDate) ?? pullRequest.createdAt

                var bucket = buckets[periodKey] ?? (periodStartDate, 0, 0, [], [])
                bucket.opened += 1

                if let firstApprovalAt = pullRequest.firstApprovalAt,
                   let approvalHours = Self.timeDifferenceInHours(startTimestamp: pullRequest.createdAt, endTimestamp: firstApprovalAt)
                {
                    bucket.approvalHours.append(approvalHours)
                }

                buckets[periodKey] = bucket
            }

            if let mergedAt = pullRequest.mergedAt,
               let mergedAtDate = Timestamp.parse(mergedAt),
               dateBounds.includes(mergedAtDate)
            {
                let periodStartDate = Self.periodStart(for: mergedAtDate, granularity: granularity)
                let periodKey = Timestamp.format(periodStartDate) ?? mergedAt

                var bucket = buckets[periodKey] ?? (periodStartDate, 0, 0, [], [])
                bucket.merged += 1

                if let mergeHours = Self.timeDifferenceInHours(startTimestamp: pullRequest.createdAt, endTimestamp: mergedAt) {
                    bucket.mergeHours.append(mergeHours)
                }

                buckets[periodKey] = bucket
            }
        }

        return buckets
            .values
            .sorted { $0.periodStartDate < $1.periodStartDate }
            .map { bucket in
                TrendRow(
                    periodStart: Timestamp.format(bucket.periodStartDate) ?? "",
                    periodGranularity: granularity.rawValue,
                    pullRequestsOpened: bucket.opened,
                    pullRequestsMerged: bucket.merged,
                    timeToMergeP50Hours: Self.percentile(bucket.mergeHours, percentile: 0.50),
                    timeToMergeP90Hours: Self.percentile(bucket.mergeHours, percentile: 0.90),
                    timeToFirstApprovalP50Hours: Self.percentile(bucket.approvalHours, percentile: 0.50),
                    timeToFirstApprovalP90Hours: Self.percentile(bucket.approvalHours, percentile: 0.90),
                    mergeSampleSize: bucket.mergeHours.count,
                    approvalSampleSize: bucket.approvalHours.count
                )
            }
    }

    private static func periodStart(for date: Date, granularity: TrendGranularity) -> Date {
        var calendar = Calendar(identifier: .iso8601)
        calendar.timeZone = TimeZone(secondsFromGMT: 0) ?? .current

        switch granularity {
        case .week:
            let components = calendar.dateComponents([.yearForWeekOfYear, .weekOfYear], from: date)
            return calendar.date(from: components) ?? date
        case .month:
            let components = calendar.dateComponents([.year, .month], from: date)
            return calendar.date(from: components) ?? date
        }
    }

    private static func timeDifferenceInHours(startTimestamp: String, endTimestamp: String?) -> Double? {
        guard
            let endTimestamp,
            let startDate = Timestamp.parse(startTimestamp),
            let endDate = Timestamp.parse(endTimestamp),
            endDate >= startDate
        else {
            return nil
        }

        return endDate.timeIntervalSince(startDate) / 3600.0
    }

    private static func percentile(_ values: [Double], percentile: Double) -> Double? {
        guard !values.isEmpty else {
            return nil
        }

        let sortedValues = values.sorted()
        let index = max(0, min(sortedValues.count - 1, Int(ceil(percentile * Double(sortedValues.count))) - 1))
        return sortedValues[index]
    }

    private static func formatDecimal(_ value: Double) -> String {
        String(format: "%.4f", value)
    }

    private func csvEscape(_ value: String?) -> String {
        guard let value else {
            return ""
        }

        if value.contains(",") || value.contains("\"") || value.contains("\n") {
            let escaped = value.replacingOccurrences(of: "\"", with: "\"\"")
            return "\"\(escaped)\""
        }

        return value
    }
}

import AppKit
import Charts
import Foundation
import SwiftUI

private struct FunnelStageValue: Identifiable {
    let periodStart: Date
    let stage: String
    let pullRequestCount: Int

    var id: String {
        "\(stage)-\(Timestamp.format(periodStart) ?? periodStart.description)"
    }
}

private struct DiagonalGuidePoint: Identifiable {
    let x: Double
    let y: Double

    var id: String {
        "\(x)-\(y)"
    }
}

private struct InsightsDashboardView: View {
    let repository: String
    let granularity: TrendGranularity
    let funnelRows: [FunnelRow]
    let cycleDistributionRows: [CycleDistributionRow]
    let lagCorrelationPoints: [LagCorrelationPoint]
    let openPullRequestAgeBuckets: [OpenPullRequestAgeBucket]

    private var chartTitle: String {
        "Pull Request Insights"
    }

    private var chartSubtitle: String {
        "Repository: \(repository)  Granularity: \(granularity.rawValue)"
    }

    private var funnelStageValues: [FunnelStageValue] {
        funnelRows.flatMap { row in
            [
                FunnelStageValue(periodStart: row.periodStart, stage: "Opened", pullRequestCount: row.pullRequestsOpened),
                FunnelStageValue(periodStart: row.periodStart, stage: "First approved", pullRequestCount: row.pullRequestsFirstApproved),
                FunnelStageValue(periodStart: row.periodStart, stage: "Merged", pullRequestCount: row.pullRequestsMerged),
                FunnelStageValue(periodStart: row.periodStart, stage: "Closed without merge", pullRequestCount: row.pullRequestsClosedWithoutMerge)
            ]
        }
    }

    private var lagGuidePoints: [DiagonalGuidePoint] {
        let maximumApprovalLag = lagCorrelationPoints.map { $0.approvalLagHours }.max() ?? 0
        let maximumMergeLag = lagCorrelationPoints.map { $0.mergeLagHours }.max() ?? 0
        let maximumLag = max(maximumApprovalLag, maximumMergeLag)

        guard maximumLag > 0 else {
            return []
        }

        return [DiagonalGuidePoint(x: 0, y: 0), DiagonalGuidePoint(x: maximumLag, y: maximumLag)]
    }

    var body: some View {
        ZStack {
            LinearGradient(
                colors: [Color(red: 0.98, green: 0.99, blue: 1.0), Color.white],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            VStack(alignment: .leading, spacing: 16) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(chartTitle)
                        .font(.system(size: 30, weight: .bold))
                    Text(chartSubtitle)
                        .font(.system(size: 14, weight: .medium))
                        .foregroundStyle(.secondary)
                }

                VStack(spacing: 16) {
                    HStack(spacing: 16) {
                        funnelChartCard
                        cycleDistributionChartCard
                    }

                    HStack(spacing: 16) {
                        lagCorrelationChartCard
                        openPullRequestAgeChartCard
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
            .padding(22)
        }
    }

    private var funnelChartCard: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Funnel by cohort period")
                .font(.system(size: 16, weight: .semibold))

            Chart {
                ForEach(funnelStageValues) { value in
                    BarMark(
                        x: .value("Period start", value.periodStart),
                        y: .value("Pull requests", value.pullRequestCount)
                    )
                    .foregroundStyle(by: .value("Stage", value.stage))
                    .position(by: .value("Stage", value.stage))
                }
            }
            .frame(height: 290)
            .chartLegend(position: .top, alignment: .leading)
            .chartForegroundStyleScale([
                "Opened": Color.blue,
                "First approved": Color.indigo,
                "Merged": Color.green,
                "Closed without merge": Color.red
            ])
            .chartXAxis {
                AxisMarks(values: .automatic(desiredCount: 7)) {
                    AxisGridLine()
                    AxisValueLabel(format: .dateTime.year().month(.abbreviated))
                }
            }
            .chartYAxis {
                AxisMarks(position: .leading)
            }
            .chartYScale(domain: .automatic(includesZero: true))
        }
        .padding(14)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 16))
    }

    private var cycleDistributionChartCard: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Cycle time distribution by period (hours)")
                .font(.system(size: 16, weight: .semibold))

            Chart {
                ForEach(cycleDistributionRows, id: \.periodStart) { row in
                    if let mergeTimeP10Hours = row.mergeTimeP10Hours {
                        LineMark(
                            x: .value("Period start", row.periodStart),
                            y: .value("P10 merge hours", mergeTimeP10Hours)
                        )
                        .foregroundStyle(by: .value("Percentile", "P10"))
                        .lineStyle(StrokeStyle(lineWidth: 2))
                    }
                }

                ForEach(cycleDistributionRows, id: \.periodStart) { row in
                    if let mergeTimeP50Hours = row.mergeTimeP50Hours {
                        LineMark(
                            x: .value("Period start", row.periodStart),
                            y: .value("Median merge hours", mergeTimeP50Hours)
                        )
                        .foregroundStyle(by: .value("Percentile", "Median"))
                        .lineStyle(StrokeStyle(lineWidth: 3))
                    }
                }

                ForEach(cycleDistributionRows, id: \.periodStart) { row in
                    if let mergeTimeP90Hours = row.mergeTimeP90Hours {
                        LineMark(
                            x: .value("Period start", row.periodStart),
                            y: .value("P90 merge hours", mergeTimeP90Hours)
                        )
                        .foregroundStyle(by: .value("Percentile", "P90"))
                        .lineStyle(StrokeStyle(lineWidth: 2))
                    }
                }
            }
            .frame(height: 290)
            .chartLegend(position: .top, alignment: .leading)
            .chartForegroundStyleScale([
                "P10": Color.teal,
                "Median": Color.orange,
                "P90": Color.red
            ])
            .chartXAxis {
                AxisMarks(values: .automatic(desiredCount: 7)) {
                    AxisGridLine()
                    AxisValueLabel(format: .dateTime.year().month(.abbreviated))
                }
            }
            .chartYAxis {
                AxisMarks(position: .leading)
            }
            .chartYScale(domain: .automatic(includesZero: true))
        }
        .padding(14)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 16))
    }

    private var lagCorrelationChartCard: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Approval lag vs merge lag (hours)")
                .font(.system(size: 16, weight: .semibold))

            Chart {
                ForEach(lagGuidePoints) { point in
                    LineMark(
                        x: .value("Approval lag", point.x),
                        y: .value("Merge lag", point.y)
                    )
                    .foregroundStyle(.gray.opacity(0.6))
                    .lineStyle(StrokeStyle(lineWidth: 1.5, dash: [4, 4]))
                }

                ForEach(Array(lagCorrelationPoints.enumerated()), id: \.offset) { entry in
                    PointMark(
                        x: .value("Approval lag (hours)", entry.element.approvalLagHours),
                        y: .value("Merge lag (hours)", entry.element.mergeLagHours)
                    )
                    .foregroundStyle(Color.blue.opacity(0.75))
                    .symbolSize(22)
                }
            }
            .frame(height: 290)
            .chartXAxis {
                AxisMarks(values: .automatic(desiredCount: 6)) {
                    AxisGridLine()
                    AxisValueLabel()
                }
            }
            .chartYAxis {
                AxisMarks(position: .leading) {
                    AxisGridLine()
                    AxisValueLabel()
                }
            }
            .chartYScale(domain: .automatic(includesZero: true))
            .chartXScale(domain: .automatic(includesZero: true))
        }
        .padding(14)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 16))
    }

    private var openPullRequestAgeChartCard: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Open pull request age buckets")
                .font(.system(size: 16, weight: .semibold))

            Chart(openPullRequestAgeBuckets, id: \.label) { bucket in
                BarMark(
                    x: .value("Age bucket", bucket.label),
                    y: .value("Open pull requests", bucket.pullRequestCount)
                )
                .foregroundStyle(Color.purple.opacity(0.75))
            }
            .frame(height: 290)
            .chartXAxis {
                AxisMarks(values: openPullRequestAgeBuckets.map { $0.label }) {
                    AxisGridLine()
                    AxisValueLabel()
                }
            }
            .chartYAxis {
                AxisMarks(position: .leading)
            }
            .chartYScale(domain: .automatic(includesZero: true))
        }
        .padding(14)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 16))
    }
}

final class InsightsChartRenderer {
    func render(
        repository: String,
        granularity: TrendGranularity,
        funnelRows: [FunnelRow],
        cycleDistributionRows: [CycleDistributionRow],
        lagCorrelationPoints: [LagCorrelationPoint],
        openPullRequestAgeBuckets: [OpenPullRequestAgeBucket],
        imageSize: CGSize,
        outputPath: String
    ) throws {
        if Thread.isMainThread {
            try MainActor.assumeIsolated {
                try Self.renderOnMainThread(
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
            return
        }

        var renderingResult: Result<Void, Error>?
        DispatchQueue.main.sync {
            renderingResult = Result {
                try MainActor.assumeIsolated {
                    try Self.renderOnMainThread(
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
            }
        }

        guard let renderingResult else {
            throw AnalyticsError.message("Unable to render chart image")
        }

        try renderingResult.get()
    }

    @MainActor
    private static func renderOnMainThread(
        repository: String,
        granularity: TrendGranularity,
        funnelRows: [FunnelRow],
        cycleDistributionRows: [CycleDistributionRow],
        lagCorrelationPoints: [LagCorrelationPoint],
        openPullRequestAgeBuckets: [OpenPullRequestAgeBucket],
        imageSize: CGSize,
        outputPath: String
    ) throws {
        let dashboardView = InsightsDashboardView(
            repository: repository,
            granularity: granularity,
            funnelRows: funnelRows,
            cycleDistributionRows: cycleDistributionRows,
            lagCorrelationPoints: lagCorrelationPoints,
            openPullRequestAgeBuckets: openPullRequestAgeBuckets
        )
        .frame(width: imageSize.width, height: imageSize.height)

        let imageRenderer = ImageRenderer(content: dashboardView)
        imageRenderer.scale = NSScreen.main?.backingScaleFactor ?? 2.0

        guard let chartImage = imageRenderer.cgImage else {
            throw AnalyticsError.message("Unable to render chart image")
        }

        let bitmapRepresentation = NSBitmapImageRep(cgImage: chartImage)
        guard let imageData = bitmapRepresentation.representation(using: .png, properties: [:]) else {
            throw AnalyticsError.message("Unable to encode chart image as PNG")
        }

        let outputURL = URL(fileURLWithPath: outputPath)
        let outputDirectoryURL = outputURL.deletingLastPathComponent()
        if !FileManager.default.fileExists(atPath: outputDirectoryURL.path) {
            try FileManager.default.createDirectory(at: outputDirectoryURL, withIntermediateDirectories: true)
        }

        try imageData.write(to: outputURL, options: .atomic)
    }
}

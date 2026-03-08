import AppKit
import Charts
import Foundation
import SwiftUI

private struct TrendChartPoint: Identifiable {
    let periodStart: Date
    let pullRequestsOpened: Int
    let pullRequestsMerged: Int
    let medianTimeToMergeHours: Double?
    let medianTimeToFirstApprovalHours: Double?

    var id: String {
        Timestamp.format(periodStart) ?? "\(periodStart.timeIntervalSince1970)"
    }
}

private struct TrendDashboardView: View {
    let repository: String
    let granularity: TrendGranularity
    let points: [TrendChartPoint]

    private var chartTitle: String {
        "Pull Request Progression"
    }

    private var chartSubtitle: String {
        "Repository: \(repository)  Granularity: \(granularity.rawValue)"
    }

    var body: some View {
        ZStack {
            LinearGradient(
                colors: [Color(red: 0.96, green: 0.98, blue: 1.0), Color.white],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            VStack(alignment: .leading, spacing: 20) {
                VStack(alignment: .leading, spacing: 6) {
                    Text(chartTitle)
                        .font(.system(size: 34, weight: .bold))
                    Text(chartSubtitle)
                        .font(.system(size: 16, weight: .medium))
                        .foregroundStyle(.secondary)
                }

                if points.isEmpty {
                    Text("No trend data is available for the selected date range.")
                        .font(.system(size: 20, weight: .semibold))
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)
                } else {
                    VStack(alignment: .leading, spacing: 24) {
                        pullRequestVolumeChart
                        durationChart
                    }
                }
            }
            .padding(28)
        }
    }

    private var pullRequestVolumeChart: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Volume by period")
                .font(.system(size: 17, weight: .semibold))

            Chart {
                ForEach(points) { point in
                    LineMark(
                        x: .value("Period start", point.periodStart),
                        y: .value("Opened", point.pullRequestsOpened)
                    )
                    .foregroundStyle(by: .value("Metric", "Opened"))
                    .lineStyle(StrokeStyle(lineWidth: 3))
                }

                ForEach(points) { point in
                    LineMark(
                        x: .value("Period start", point.periodStart),
                        y: .value("Merged", point.pullRequestsMerged)
                    )
                    .foregroundStyle(by: .value("Metric", "Merged"))
                    .lineStyle(StrokeStyle(lineWidth: 3))
                }
            }
            .frame(height: 260)
            .chartForegroundStyleScale(["Opened": Color.blue, "Merged": Color.green])
            .chartLegend(position: .top, alignment: .leading)
            .chartXAxis {
                AxisMarks(values: .automatic(desiredCount: 8)) {
                    AxisGridLine()
                    AxisValueLabel(format: .dateTime.year().month(.abbreviated))
                }
            }
            .chartYAxis {
                AxisMarks(position: .leading)
            }
            .chartYScale(domain: .automatic(includesZero: true))
        }
        .padding(16)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 16))
    }

    private var durationChart: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Median durations (hours)")
                .font(.system(size: 17, weight: .semibold))

            Chart {
                ForEach(points) { point in
                    if let medianTimeToMergeHours = point.medianTimeToMergeHours {
                        LineMark(
                            x: .value("Period start", point.periodStart),
                            y: .value("Median merge hours", medianTimeToMergeHours)
                        )
                        .foregroundStyle(by: .value("Metric", "Median merge hours"))
                        .lineStyle(StrokeStyle(lineWidth: 3))
                    }
                }

                ForEach(points) { point in
                    if let medianTimeToFirstApprovalHours = point.medianTimeToFirstApprovalHours {
                        LineMark(
                            x: .value("Period start", point.periodStart),
                            y: .value("Median first approval hours", medianTimeToFirstApprovalHours)
                        )
                        .foregroundStyle(by: .value("Metric", "Median first approval hours"))
                        .lineStyle(StrokeStyle(lineWidth: 3))
                    }
                }
            }
            .frame(height: 260)
            .chartForegroundStyleScale([
                "Median merge hours": Color.orange,
                "Median first approval hours": Color.pink
            ])
            .chartLegend(position: .top, alignment: .leading)
            .chartXAxis {
                AxisMarks(values: .automatic(desiredCount: 8)) {
                    AxisGridLine()
                    AxisValueLabel(format: .dateTime.year().month(.abbreviated))
                }
            }
            .chartYAxis {
                AxisMarks(position: .leading)
            }
            .chartYScale(domain: .automatic(includesZero: true))
        }
        .padding(16)
        .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 16))
    }
}

final class TrendChartRenderer {
    func render(
        repository: String,
        granularity: TrendGranularity,
        trendRows: [TrendRow],
        imageSize: CGSize,
        outputPath: String
    ) throws {
        let points = trendRows
            .compactMap { trendRow -> TrendChartPoint? in
                guard let periodStart = Timestamp.parse(trendRow.periodStart) else {
                    return nil
                }

                return TrendChartPoint(
                    periodStart: periodStart,
                    pullRequestsOpened: trendRow.pullRequestsOpened,
                    pullRequestsMerged: trendRow.pullRequestsMerged,
                    medianTimeToMergeHours: trendRow.timeToMergeP50Hours,
                    medianTimeToFirstApprovalHours: trendRow.timeToFirstApprovalP50Hours
                )
            }
            .sorted(by: { $0.periodStart < $1.periodStart })

        if Thread.isMainThread {
            try MainActor.assumeIsolated {
                try Self.renderOnMainThread(
                    repository: repository,
                    granularity: granularity,
                    points: points,
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
                        points: points,
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
        points: [TrendChartPoint],
        imageSize: CGSize,
        outputPath: String
    ) throws {
        let dashboardView = TrendDashboardView(repository: repository, granularity: granularity, points: points)
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

import ArgumentParser
import Foundation

struct RepositoryOptions: ParsableArguments {
    @Option(name: .long, help: "Repository in owner/name format. Defaults to the repository for the current working directory.")
    var repo: String?

    @Option(name: .customLong("database-path"), help: "Path to the local analytics store file.")
    var databasePath: String?
}

struct DateRangeOptions: ParsableArguments {
    @Option(name: .customLong("from-date"), help: "Inclusive start date in YYYY-MM-DD format.")
    var fromDate: String?

    @Option(name: .customLong("to-date"), help: "Inclusive end date in YYYY-MM-DD format.")
    var toDate: String?
}

struct GHPRAnalyticsCommand: ParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "gh-pr-analytics",
        abstract: "Historical pull request process analytics.",
        discussion: "First approval is defined as the earliest observed APPROVED review submission timestamp for a pull request.",
        subcommands: [Setup.self, Initialize.self, Synchronize.self, Report.self, Export.self, Status.self]
    )

    struct Setup: ParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "setup",
            abstract: "Clone a repository into a dedicated analytics workspace and initialize local storage."
        )

        @Option(name: .long, help: "Repository in owner/name format.")
        var repo: String

        @Option(name: .customLong("workspace-root"), help: "Directory where workspace subdirectories are created.")
        var workspaceRoot: String = ".gh-pr-analytics/workspaces"

        @Option(name: .customLong("database-path"), help: "Optional path to the local analytics store file.")
        var databasePath: String?

        mutating func run() throws {
            let application = Application()
            try application.setupWorkspace(
                repository: repo,
                workspaceRootOverride: workspaceRoot,
                databasePathOverride: databasePath
            )
        }
    }

    struct Initialize: ParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "init",
            abstract: "Initialize local analytics storage for a repository."
        )

        @OptionGroup var repositoryOptions: RepositoryOptions

        mutating func run() throws {
            let application = Application()
            try application.initialize(
                repositoryOverride: repositoryOptions.repo,
                databasePathOverride: repositoryOptions.databasePath
            )
        }
    }

    struct Synchronize: ParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "sync",
            abstract: "Collect pull request metadata and review history from GitHub."
        )

        @OptionGroup var repositoryOptions: RepositoryOptions

        @Option(name: .customLong("phase"), help: "Synchronization phase: metadata, reviews, or all.")
        var phase: String = "all"

        @Flag(name: .customLong("backfill"), help: "Run in backfill mode.")
        var backfill = false

        @Flag(name: .customLong("resume"), help: "Resume from saved checkpoints when available.")
        var resume = false

        @Flag(name: .customLong("skip-closed-unmerged"), help: "Skip review enrichment for pull requests that are closed without merge.")
        var skipClosedUnmerged = false

        @OptionGroup var dateRangeOptions: DateRangeOptions

        @Option(name: .customLong("batch-size"), help: "Batch size for pull request processing (1-100).")
        var batchSize = 100

        @Option(name: .customLong("max-pages"), help: "Maximum number of pages to fetch.")
        var maxPages: Int?

        mutating func run() throws {
            let application = Application()
            try application.synchronize(
                repositoryOverride: repositoryOptions.repo,
                databasePathOverride: repositoryOptions.databasePath,
                phaseValue: phase,
                backfill: backfill,
                resume: resume,
                skipClosedUnmergedPullRequests: skipClosedUnmerged,
                fromDate: dateRangeOptions.fromDate,
                toDate: dateRangeOptions.toDate,
                batchSize: batchSize,
                maximumPages: maxPages
            )
        }
    }

    struct Report: ParsableCommand {
        static let configuration = CommandConfiguration(
            abstract: "Generate reports from the local analytics store.",
            subcommands: [Trends.self, Charts.self]
        )

        struct Trends: ParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "trends",
                abstract: "Report trend aggregates by week or month."
            )

            @OptionGroup var repositoryOptions: RepositoryOptions
            @OptionGroup var dateRangeOptions: DateRangeOptions

            @Option(name: .customLong("granularity"), help: "Trend aggregation granularity: week or month.")
            var granularity: String = "week"

            @Option(name: .customLong("format"), help: "Output format: csv or json.")
            var format: String = "csv"

            mutating func run() throws {
                let application = Application()
                try application.reportTrends(
                    repositoryOverride: repositoryOptions.repo,
                    databasePathOverride: repositoryOptions.databasePath,
                    granularityValue: granularity,
                    formatValue: format,
                    fromDate: dateRangeOptions.fromDate,
                    toDate: dateRangeOptions.toDate
                )
            }
        }

        struct Charts: ParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "charts",
                abstract: "Render trend progression charts to a PNG file."
            )

            @OptionGroup var repositoryOptions: RepositoryOptions
            @OptionGroup var dateRangeOptions: DateRangeOptions

            @Option(name: .customLong("granularity"), help: "Trend aggregation granularity: week or month.")
            var granularity: String = "week"

            @Option(name: .customLong("output-path"), help: "Path to the PNG image file.")
            var outputPath: String = "trend-progression.png"

            @Option(name: .customLong("width"), help: "Chart image width in pixels.")
            var width: Int = 1400

            @Option(name: .customLong("height"), help: "Chart image height in pixels.")
            var height: Int = 900

            mutating func run() throws {
                let application = Application()
                try application.reportCharts(
                    repositoryOverride: repositoryOptions.repo,
                    databasePathOverride: repositoryOptions.databasePath,
                    granularityValue: granularity,
                    fromDate: dateRangeOptions.fromDate,
                    toDate: dateRangeOptions.toDate,
                    outputPath: outputPath,
                    width: width,
                    height: height
                )
            }
        }
    }

    struct Export: ParsableCommand {
        static let configuration = CommandConfiguration(
            abstract: "Export records from the local analytics store.",
            subcommands: [PullRequests.self]
        )

        struct PullRequests: ParsableCommand {
            static let configuration = CommandConfiguration(
                commandName: "pull-requests",
                abstract: "Export one row per pull request.",
                aliases: ["prs"]
            )

            @OptionGroup var repositoryOptions: RepositoryOptions

            @Option(name: .customLong("format"), help: "Output format: csv or json.")
            var format: String = "csv"

            mutating func run() throws {
                let application = Application()
                try application.exportPullRequests(
                    repositoryOverride: repositoryOptions.repo,
                    databasePathOverride: repositoryOptions.databasePath,
                    formatValue: format
                )
            }
        }
    }

    struct Status: ParsableCommand {
        static let configuration = CommandConfiguration(
            commandName: "status",
            abstract: "Show repository and synchronization status from the local analytics store."
        )

        @OptionGroup var repositoryOptions: RepositoryOptions

        mutating func run() throws {
            let application = Application()
            try application.status(
                repositoryOverride: repositoryOptions.repo,
                databasePathOverride: repositoryOptions.databasePath
            )
        }
    }
}

GHPRAnalyticsCommand.main()

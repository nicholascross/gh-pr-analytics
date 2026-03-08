import Foundation
import Darwin

private final class ProcessOutputBuffer: @unchecked Sendable {
    private let lock = NSLock()
    private var data = Data()

    func append(_ chunk: Data) {
        lock.lock()
        data.append(chunk)
        lock.unlock()
    }

    func snapshot() -> Data {
        lock.lock()
        let value = data
        lock.unlock()
        return value
    }
}

private final class URLSessionResponseBuffer: @unchecked Sendable {
    private let lock = NSLock()
    private var data = Data()
    private var response: URLResponse?
    private var error: Error?

    func set(data: Data, response: URLResponse?, error: Error?) {
        lock.lock()
        self.data = data
        self.response = response
        self.error = error
        lock.unlock()
    }

    func snapshot() -> (Data, URLResponse?, Error?) {
        lock.lock()
        let snapshot = (data, response, error)
        lock.unlock()
        return snapshot
    }
}

struct ProcessRunResult {
    let standardOutput: Data
    let standardError: Data
    let exitCode: Int32

    var standardOutputString: String {
        String(data: standardOutput, encoding: .utf8) ?? ""
    }

    var standardErrorString: String {
        String(data: standardError, encoding: .utf8) ?? ""
    }
}

final class ProcessRunner {
    func run(command: String, arguments: [String], timeoutSeconds: TimeInterval? = nil) throws -> ProcessRunResult {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = [command] + arguments

        // Use a fresh /dev/null handle for each process invocation.
        // Reusing a shared handle can eventually produce EBADF on repeated launches.
        let nullInputHandle = try FileHandle(forReadingFrom: URL(fileURLWithPath: "/dev/null"))
        defer { try? nullInputHandle.close() }
        process.standardInput = nullInputHandle

        var environment = ProcessInfo.processInfo.environment
        environment["GH_PROMPT_DISABLED"] = "1"
        environment["GIT_TERMINAL_PROMPT"] = "0"
        environment["GH_NO_UPDATE_NOTIFIER"] = "1"
        process.environment = environment

        let standardOutputPipe = Pipe()
        let standardErrorPipe = Pipe()
        process.standardOutput = standardOutputPipe
        process.standardError = standardErrorPipe
        let standardOutputHandle = standardOutputPipe.fileHandleForReading
        let standardErrorHandle = standardErrorPipe.fileHandleForReading

        let standardOutputBuffer = ProcessOutputBuffer()
        let standardErrorBuffer = ProcessOutputBuffer()

        standardOutputHandle.readabilityHandler = { handle in
            let chunk = handle.availableData
            guard !chunk.isEmpty else {
                return
            }

            standardOutputBuffer.append(chunk)
        }

        standardErrorHandle.readabilityHandler = { handle in
            let chunk = handle.availableData
            guard !chunk.isEmpty else {
                return
            }

            standardErrorBuffer.append(chunk)
        }

        do {
            try process.run()
        } catch {
            throw AnalyticsError.message("Failed to execute \(command): \(error.localizedDescription)")
        }

        var didTimeout = false
        if let timeoutSeconds {
            didTimeout = waitForExit(process: process, command: command, timeoutSeconds: timeoutSeconds)
        } else {
            process.waitUntilExit()
        }

        standardOutputHandle.readabilityHandler = nil
        standardErrorHandle.readabilityHandler = nil

        let remainingStandardOutputData = standardOutputHandle.readDataToEndOfFile()
        let remainingStandardErrorData = standardErrorHandle.readDataToEndOfFile()

        standardOutputBuffer.append(remainingStandardOutputData)
        standardErrorBuffer.append(remainingStandardErrorData)

        let standardOutputData = standardOutputBuffer.snapshot()
        let standardErrorData = standardErrorBuffer.snapshot()

        if didTimeout {
            let standardErrorText = String(data: standardErrorData, encoding: .utf8) ?? ""
            let standardOutputText = String(data: standardOutputData, encoding: .utf8) ?? ""
            let summarizedStandardError = summarizeProcessOutput(standardErrorText)
            let summarizedStandardOutput = summarizeProcessOutput(standardOutputText)

            var details = "Command \(command) timed out after \(Int(timeoutSeconds ?? 0)) seconds."
            if !summarizedStandardError.isEmpty {
                details += " Partial stderr: \(summarizedStandardError)"
            }
            if !summarizedStandardOutput.isEmpty {
                details += " Partial stdout: \(summarizedStandardOutput)"
            }
            details += " Ensure GitHub CLI authentication is valid and that outbound access to api.github.com is available."

            throw AnalyticsError.message(details)
        }

        return ProcessRunResult(
            standardOutput: standardOutputData,
            standardError: standardErrorData,
            exitCode: process.terminationStatus
        )
    }

    private func waitForExit(process: Process, command: String, timeoutSeconds: TimeInterval) -> Bool {
        let timeoutDate = Date().addingTimeInterval(timeoutSeconds)
        let startDate = Date()
        var nextHeartbeatDate = Date().addingTimeInterval(10.0)

        while process.isRunning, Date() < timeoutDate {
            let now = Date()
            if now >= nextHeartbeatDate {
                let elapsedSeconds = Int(now.timeIntervalSince(startDate))
                fputs("Still waiting for \(command) command to complete (\(elapsedSeconds) seconds elapsed).\n", stderr)
                nextHeartbeatDate = now.addingTimeInterval(10.0)
            }
            Thread.sleep(forTimeInterval: 0.2)
        }

        guard process.isRunning else {
            return false
        }

        process.terminate()
        Thread.sleep(forTimeInterval: 0.5)

        if process.isRunning {
            kill(process.processIdentifier, SIGKILL)
            Thread.sleep(forTimeInterval: 0.2)
        }
        return true
    }

    private func summarizeProcessOutput(_ value: String, maximumLength: Int = 600) -> String {
        let condensed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard condensed.count > maximumLength else {
            return condensed
        }

        let endIndex = condensed.index(condensed.startIndex, offsetBy: maximumLength)
        return "\(condensed[..<endIndex])..."
    }
}

final class GitHubClient {
    private let processRunner: ProcessRunner
    private let jsonDecoder: JSONDecoder
    private let urlSession: URLSession
    private let minimumRequestDelaySeconds: TimeInterval
    private let requestTimeoutSeconds: TimeInterval
    private let maximumRequestAttempts: Int
    private var lastRequestStartDate: Date?
    private var warnedAboutMissingAuthentication = false
    private lazy var authorizationToken: String? = resolveAuthorizationToken()

    var minimumRequestDelayMilliseconds: Int {
        Int((minimumRequestDelaySeconds * 1000.0).rounded())
    }

    var requestTimeoutMilliseconds: Int {
        Int((requestTimeoutSeconds * 1000.0).rounded())
    }

    var maximumAttempts: Int {
        maximumRequestAttempts
    }

    init(processRunner: ProcessRunner = ProcessRunner()) {
        self.processRunner = processRunner
        self.jsonDecoder = JSONDecoder()
        self.minimumRequestDelaySeconds = Self.resolveMinimumRequestDelaySeconds()
        self.requestTimeoutSeconds = Self.resolveRequestTimeoutSeconds()
        self.maximumRequestAttempts = Self.resolveMaximumRequestAttempts()
        self.lastRequestStartDate = nil

        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = requestTimeoutSeconds
        configuration.timeoutIntervalForResource = requestTimeoutSeconds + 5.0
        configuration.waitsForConnectivity = false
        self.urlSession = URLSession(configuration: configuration)
    }

    func resolveRepositoryFromCurrentDirectory() throws -> String {
        let response = try processRunner.run(
            command: "gh",
            arguments: ["repo", "view", "--json", "nameWithOwner", "-q", ".nameWithOwner"]
        )

        guard response.exitCode == 0 else {
            throw AnalyticsError.message(
                "Unable to resolve repository from current directory. Pass --repo owner/name. Details: \(response.standardErrorString)"
            )
        }

        let repository = response.standardOutputString.trimmingCharacters(in: .whitespacesAndNewlines)

        guard repository.contains("/") else {
            throw AnalyticsError.message("Unexpected repository format from gh: \(repository)")
        }

        return repository
    }

    func fetchPullRequestMetadata(
        repository: String,
        sort: String,
        direction: String,
        page: Int,
        perPage: Int
    ) throws -> [PullRequestMetadata] {
        let endpoint = try pullRequestEndpoint(repository: repository)
        let data = try runGitHubRESTAPI(
            endpoint: endpoint,
            queryItems: [
                URLQueryItem(name: "state", value: "all"),
                URLQueryItem(name: "sort", value: sort),
                URLQueryItem(name: "direction", value: direction),
                URLQueryItem(name: "per_page", value: String(perPage)),
                URLQueryItem(name: "page", value: String(page))
            ]
        )

        do {
            return try jsonDecoder.decode([PullRequestMetadata].self, from: data)
        } catch {
            throw AnalyticsError.message("Unable to decode pull request metadata page \(page): \(error.localizedDescription)")
        }
    }

    func fetchPullRequestReviews(
        repository: String,
        pullRequestNumber: Int,
        page: Int,
        perPage: Int
    ) throws -> [PullRequestReview] {
        let endpoint = try reviewEndpoint(repository: repository, pullRequestNumber: pullRequestNumber)
        let data = try runGitHubRESTAPI(
            endpoint: endpoint,
            queryItems: [
                URLQueryItem(name: "per_page", value: String(perPage)),
                URLQueryItem(name: "page", value: String(page))
            ]
        )

        do {
            return try jsonDecoder.decode([PullRequestReview].self, from: data)
        } catch {
            throw AnalyticsError.message(
                "Unable to decode review payload for pull request #\(pullRequestNumber) page \(page): \(error.localizedDescription)"
            )
        }
    }

    func cloneRepository(repository: String, destinationPath: String) throws {
        let response = try processRunner.run(
            command: "gh",
            arguments: ["repo", "clone", repository, destinationPath]
        )

        guard response.exitCode == 0 else {
            throw AnalyticsError.message(
                "Unable to clone repository \(repository) into \(destinationPath). Details: \(response.standardErrorString)"
            )
        }
    }

    private func runGitHubRESTAPI(endpoint: String, queryItems: [URLQueryItem]) throws -> Data {
        var attempt = 1

        while true {
            applyRequestPacingIfNeeded()
            let request = try buildRESTRequest(endpoint: endpoint, queryItems: queryItems)

            do {
                let (data, response) = try executeRESTRequest(request)
                guard let httpResponse = response as? HTTPURLResponse else {
                    throw AnalyticsError.message("GitHub API call failed: no HTTP response")
                }

                if (200 ... 299).contains(httpResponse.statusCode) {
                    return data
                }

                let responseBody = String(data: data, encoding: .utf8) ?? ""
                let lowercasedResponseBody = responseBody.lowercased()
                let retryableStatusCodes: Set<Int> = [429, 500, 502, 503, 504]
                let isRetryable =
                    retryableStatusCodes.contains(httpResponse.statusCode) ||
                    lowercasedResponseBody.contains("secondary rate limit") ||
                    lowercasedResponseBody.contains("rate limit")

                if isRetryable, attempt < maximumRequestAttempts {
                    let delaySeconds = pow(2.0, Double(attempt - 1))
                    fputs(
                        "Retrying GitHub API call after HTTP \(httpResponse.statusCode) (attempt \(attempt + 1)/\(maximumRequestAttempts)).\n",
                        stderr
                    )
                    Thread.sleep(forTimeInterval: delaySeconds)
                    attempt += 1
                    continue
                }

                let summarizedResponseBody = summarizeProcessOutput(responseBody)
                throw AnalyticsError.message(
                    "GitHub API call failed with HTTP \(httpResponse.statusCode): \(summarizedResponseBody)"
                )
            } catch {
                let lowercasedErrorDescription = String(describing: error).lowercased()
                let isRetryable =
                    lowercasedErrorDescription.contains("timed out") ||
                    lowercasedErrorDescription.contains("network connection") ||
                    lowercasedErrorDescription.contains("temporarily unavailable")

                if isRetryable, attempt < maximumRequestAttempts {
                    let delaySeconds = pow(2.0, Double(attempt - 1))
                    fputs(
                        "GitHub API call failed with transient network error. Retrying (attempt \(attempt + 1)/\(maximumRequestAttempts)).\n",
                        stderr
                    )
                    Thread.sleep(forTimeInterval: delaySeconds)
                    attempt += 1
                    continue
                }

                throw error
            }
        }
    }

    private func pullRequestEndpoint(repository: String) throws -> String {
        let components = repository.split(separator: "/", maxSplits: 1, omittingEmptySubsequences: true)

        guard components.count == 2 else {
            throw AnalyticsError.message("Repository must be in owner/name format. Received: \(repository)")
        }

        return "repos/\(components[0])/\(components[1])/pulls"
    }

    private func reviewEndpoint(repository: String, pullRequestNumber: Int) throws -> String {
        let pullRequestListEndpoint = try pullRequestEndpoint(repository: repository)
        return "\(pullRequestListEndpoint)/\(pullRequestNumber)/reviews"
    }

    private func buildRESTRequest(endpoint: String, queryItems: [URLQueryItem]) throws -> URLRequest {
        var components = URLComponents()
        components.scheme = "https"
        components.host = "api.github.com"
        components.path = "/\(endpoint)"
        components.queryItems = queryItems

        guard let url = components.url else {
            throw AnalyticsError.message("Unable to create GitHub API URL for endpoint \(endpoint)")
        }

        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("application/vnd.github+json", forHTTPHeaderField: "Accept")
        request.setValue("2022-11-28", forHTTPHeaderField: "X-GitHub-Api-Version")
        request.setValue("gh-pr-analytics", forHTTPHeaderField: "User-Agent")

        if let token = authorizationToken {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        } else if !warnedAboutMissingAuthentication {
            warnedAboutMissingAuthentication = true
            fputs(
                "Warning: no GitHub token detected; API access is unauthenticated and may be heavily rate-limited.\n",
                stderr
            )
        }

        return request
    }

    private func executeRESTRequest(_ request: URLRequest) throws -> (Data, URLResponse) {
        let semaphore = DispatchSemaphore(value: 0)
        let responseBuffer = URLSessionResponseBuffer()

        let task = urlSession.dataTask(with: request) { data, urlResponse, error in
            responseBuffer.set(data: data ?? Data(), response: urlResponse, error: error)
            semaphore.signal()
        }
        task.resume()

        let waitDeadline = DispatchTime.now() + requestTimeoutSeconds + 2.0
        let waitResult = semaphore.wait(timeout: waitDeadline)
        if waitResult == .timedOut {
            task.cancel()
            throw AnalyticsError.message("GitHub API call timed out after \(requestTimeoutMilliseconds) milliseconds.")
        }

        let (responseData, response, responseError) = responseBuffer.snapshot()

        if let responseError {
            throw AnalyticsError.message("GitHub API call failed: \(responseError.localizedDescription)")
        }

        guard let response else {
            throw AnalyticsError.message("GitHub API call failed: missing response.")
        }

        return (responseData, response)
    }

    private func resolveAuthorizationToken() -> String? {
        let environment = ProcessInfo.processInfo.environment
        if let explicitToken = environment["GH_TOKEN"]?.trimmingCharacters(in: .whitespacesAndNewlines), !explicitToken.isEmpty {
            return explicitToken
        }

        if let explicitToken = environment["GITHUB_TOKEN"]?.trimmingCharacters(in: .whitespacesAndNewlines), !explicitToken.isEmpty {
            return explicitToken
        }

        guard let response = try? processRunner.run(
            command: "gh",
            arguments: ["auth", "token"],
            timeoutSeconds: 10
        ) else {
            return nil
        }

        guard response.exitCode == 0 else {
            return nil
        }

        let token = response.standardOutputString.trimmingCharacters(in: .whitespacesAndNewlines)
        return token.isEmpty ? nil : token
    }

    private func summarizeProcessOutput(_ value: String, maximumLength: Int = 600) -> String {
        let condensed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard condensed.count > maximumLength else {
            return condensed
        }

        let endIndex = condensed.index(condensed.startIndex, offsetBy: maximumLength)
        return "\(condensed[..<endIndex])..."
    }

    private func applyRequestPacingIfNeeded() {
        let now = Date()

        if let lastRequestStartDate {
            let elapsed = now.timeIntervalSince(lastRequestStartDate)
            if elapsed < minimumRequestDelaySeconds {
                Thread.sleep(forTimeInterval: minimumRequestDelaySeconds - elapsed)
            }
        }

        lastRequestStartDate = Date()
    }

    private static func resolveMinimumRequestDelaySeconds() -> TimeInterval {
        let defaultDelayMilliseconds = 600.0

        let environment = ProcessInfo.processInfo.environment
        guard let rawValue = environment["GH_PR_ANALYTICS_REQUEST_DELAY_MS"] else {
            return defaultDelayMilliseconds / 1000.0
        }

        guard let parsedMilliseconds = Double(rawValue), parsedMilliseconds >= 0 else {
            return defaultDelayMilliseconds / 1000.0
        }

        let boundedMilliseconds = min(parsedMilliseconds, 60_000.0)
        return boundedMilliseconds / 1000.0
    }

    private static func resolveRequestTimeoutSeconds() -> TimeInterval {
        let defaultTimeoutMilliseconds = 30_000.0

        let environment = ProcessInfo.processInfo.environment
        guard let rawValue = environment["GH_PR_ANALYTICS_REQUEST_TIMEOUT_MS"] else {
            return defaultTimeoutMilliseconds / 1000.0
        }

        guard let parsedMilliseconds = Double(rawValue), parsedMilliseconds > 0 else {
            return defaultTimeoutMilliseconds / 1000.0
        }

        let boundedMilliseconds = min(parsedMilliseconds, 600_000.0)
        return boundedMilliseconds / 1000.0
    }

    private static func resolveMaximumRequestAttempts() -> Int {
        let defaultMaximumAttempts = 2

        let environment = ProcessInfo.processInfo.environment
        guard let rawValue = environment["GH_PR_ANALYTICS_MAX_ATTEMPTS"] else {
            return defaultMaximumAttempts
        }

        guard let parsedValue = Int(rawValue) else {
            return defaultMaximumAttempts
        }

        return max(1, min(parsedValue, 10))
    }
}

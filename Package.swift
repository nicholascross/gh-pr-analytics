// swift-tools-version: 6.2
import PackageDescription

let package = Package(
    name: "gh-pr-analytics",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(name: "gh-pr-analytics", targets: ["GHPRAnalyticsCLI"])
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.3.0")
    ],
    targets: [
        .executableTarget(
            name: "GHPRAnalyticsCLI",
            dependencies: [
                .product(name: "ArgumentParser", package: "swift-argument-parser")
            ]
        )
    ]
)

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"Platform verification failed: {message}")


def main() -> None:
    parity_pairs = (
        ("infra/create-topics.sh", "infra/create-topics.ps1"),
        ("infra/verify-cluster.sh", "infra/verify-cluster.ps1"),
        ("scripts/verify-static.sh", "scripts/verify-static.ps1"),
        ("scripts/run-priority-demo.sh", "scripts/run-priority-demo.ps1"),
        ("scripts/capture-consumer-lag.sh", "scripts/capture-consumer-lag.ps1"),
        ("scripts/run-flink-incidents.sh", "scripts/run-flink-incidents.ps1"),
        ("scripts/run-spark-ward-energy.sh", "scripts/run-spark-ward-energy.ps1"),
        (
            "scripts/run-spark-health-advisories.sh",
            "scripts/run-spark-health-advisories.ps1",
        ),
        ("scripts/submission-preflight.sh", "scripts/submission-preflight.ps1"),
        ("scripts/build-submission-zip.sh", "scripts/build-submission-zip.ps1"),
    )
    for macos_path, windows_path in parity_pairs:
        require((ROOT / macos_path).is_file(), f"missing macOS launcher {macos_path}")
        require((ROOT / windows_path).is_file(), f"missing Windows launcher {windows_path}")

    compose_text = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
    require("name: urbanpulse" in compose_text, "Compose project/network name is not stable")
    require("127.0.0.1:19092:19092" in compose_text, "Kafka host port is not loopback-only")

    bash_package = (ROOT / "scripts/build-submission-zip.sh").read_text(encoding="utf-8")
    require("/private/tmp" not in bash_package, "ZIP builder contains a macOS-only temp path")
    require("${TMPDIR:-/tmp}" in bash_package, "ZIP builder does not use a portable temp root")

    for path in (ROOT / "scripts").glob("run-spark-*.ps1"):
        text = path.read_text(encoding="utf-8")
        require("--mount" in text, f"{path.name} does not use Windows-safe Docker mounts")

    attributes = (ROOT / ".gitattributes").read_text(encoding="utf-8")
    require("*.sh text eol=lf" in attributes, "Bash LF policy is missing")
    require("*.ps1 text eol=crlf" in attributes, "PowerShell checkout policy is missing")
    require((ROOT / "PLATFORM_SETUP.md").is_file(), "platform setup guide is missing")
    print("Windows/macOS platform verification passed")


if __name__ == "__main__":
    main()

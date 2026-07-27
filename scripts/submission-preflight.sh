#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

fail() {
  echo "Submission preflight failed: $1" >&2
  exit 1
}

required_files=(
  report/UrbanPulse_Submission_Report.docx
  report/UrbanPulse_Submission_Report.pdf
  evidence/cluster-verification.txt
  evidence/consumer-lag.csv
  evidence/priority-consumer-lag.png
  evidence/dlq-report.csv
  evidence/enriched-event.json
  evidence/incidents.jsonl
  evidence/ward-energy-sample.json
  evidence/parquet-partitions.txt
  evidence/health-advisory-sample.json
)

for path in "${required_files[@]}"; do
  [[ -s "$path" ]] || fail "missing or empty $path"
done

if grep -Eq '^\*\*(Student name\(s\)|Student ID\(s\)|Git repository|Video walkthrough):\*\* _+' report/report.md; then
  fail "fill every identity, Git repository, and video field in report/report.md"
fi

fixture_sha="d9fa9f86c9ea1000e30dfeedd75996a6f217f4f002990aed585b6fcd5099e0fc"
if command -v shasum >/dev/null 2>&1; then
  current_sha="$(shasum -a 256 reference-data/route_schedule.csv | awk '{print $1}')"
else
  current_sha="$(sha256sum reference-data/route_schedule.csv | awk '{print $1}')"
fi
[[ "$current_sha" != "$fixture_sha" ]] || fail "replace the development route_schedule.csv with the official eLearn file"

grep -Eq '^duration_seconds,300\r?$' evidence/dlq-report.csv || \
  fail "evidence/dlq-report.csv is not a 300-second capture"

if [[ report/report.md -nt report/UrbanPulse_Submission_Report.pdf ]]; then
  fail "report PDF is older than report/report.md; rebuild and visually verify it"
fi

./scripts/verify-static.sh
echo "Submission preflight passed"

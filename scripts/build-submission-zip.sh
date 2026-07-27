#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output="${1:-$repo_dir/UrbanPulse_Submission.zip}"

if [[ -e "$output" ]]; then
  echo "Refusing to overwrite existing file: $output" >&2
  exit 1
fi
if [[ ! -s "$repo_dir/report/UrbanPulse_Submission_Report.pdf" ]]; then
  echo "Missing final report PDF" >&2
  exit 1
fi

"$repo_dir/scripts/submission-preflight.sh"

stage_dir="$(mktemp -d "${TMPDIR:-/tmp}/urbanpulse-submission.XXXXXX")"
cleanup() { rm -rf "$stage_dir"; }
trap cleanup EXIT
package_dir="$stage_dir/UrbanPulse_Submission"
mkdir -p "$package_dir"

rsync -a \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '.env' \
  --exclude '__pycache__/' \
  --exclude '*.pyc' \
  --exclude 'checkpoints/' \
  --exclude 'output/' \
  --exclude 'report/rendered*/' \
  --exclude 'report/a11y-report*.json' \
  --exclude '*.zip' \
  "$repo_dir/" "$package_dir/"

(
  cd "$stage_dir"
  zip -q -r "$output" UrbanPulse_Submission
)
echo "Wrote $output"

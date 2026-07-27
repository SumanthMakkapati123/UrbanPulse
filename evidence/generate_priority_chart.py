"""Render the reproducible priority-consumer lag evidence chart.

Chart contract:
- Question: does the high-priority group recover faster under the same load?
- Takeaway: high-priority lag returns to zero while the throttled standard group grows.
- Form: two-series line chart across 14 ordered five-to-eleven-second samples.
- Encoding: blue solid/high versus orange dashed/standard, plus direct labels.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parent
INPUT = ROOT / "consumer-lag.csv"
OUTPUT = ROOT / "priority-consumer-lag.png"

WIDTH, HEIGHT = 1600, 900
LEFT, RIGHT, TOP, BOTTOM = 150, 1500, 205, 730
INK = "#172033"
MUTED = "#5E687B"
GRID = "#DCE2EA"
BLUE = "#1769AA"
ORANGE = "#D97706"


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for candidate in candidates:
        try:
            return ImageFont.truetype(candidate, size)
        except OSError:
            continue
    return ImageFont.load_default()


def load_rows() -> tuple[list[datetime], dict[str, list[int]]]:
    by_time: dict[datetime, dict[str, int]] = {}
    with INPUT.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            timestamp = datetime.fromisoformat(row["timestamp_utc"].replace("Z", "+00:00"))
            by_time.setdefault(timestamp, {})[row["group"]] = int(row["total_lag"])
    times = sorted(by_time)
    series = {
        "High priority": [by_time[t]["traffic-signals-high-priority"] for t in times],
        "Standard priority": [by_time[t]["traffic-signals-standard-priority"] for t in times],
    }
    return times, series


def main() -> None:
    times, series = load_rows()
    image = Image.new("RGB", (WIDTH, HEIGHT), "#FAFBFD")
    draw = ImageDraw.Draw(image)

    draw.text((LEFT, 55), "Kafka consumer-group lag under identical traffic load", font=font(38, True), fill=INK)
    draw.text(
        (LEFT, 112),
        "One unthrottled high-priority consumer vs three standard consumers with 250 ms processing delay • 120-second run",
        font=font(21),
        fill=MUTED,
    )

    y_max = 30000
    for value in range(0, y_max + 1, 5000):
        y = BOTTOM - (value / y_max) * (BOTTOM - TOP)
        draw.line((LEFT, y, RIGHT, y), fill=GRID, width=2)
        label = f"{value:,}"
        box = draw.textbbox((0, 0), label, font=font(18))
        draw.text((LEFT - 18 - (box[2] - box[0]), y - 10), label, font=font(18), fill=MUTED)

    draw.line((LEFT, TOP, LEFT, BOTTOM), fill="#8993A4", width=2)
    draw.line((LEFT, BOTTOM, RIGHT, BOTTOM), fill="#8993A4", width=2)
    draw.text((30, TOP + 195), "Total lag (records)", font=font(20, True), fill=INK)

    xs = [LEFT + i * (RIGHT - LEFT) / (len(times) - 1) for i in range(len(times))]
    for i, (x, timestamp) in enumerate(zip(xs, times)):
        if i % 2 == 0 or i == len(times) - 1:
            draw.line((x, BOTTOM, x, BOTTOM + 8), fill="#8993A4", width=2)
            draw.text((x - 30, BOTTOM + 18), timestamp.strftime("%H:%M:%S"), font=font(15), fill=MUTED)

    def points(values: list[int]) -> list[tuple[float, float]]:
        return [(x, BOTTOM - (value / y_max) * (BOTTOM - TOP)) for x, value in zip(xs, values)]

    high_points = points(series["High priority"])
    standard_points = points(series["Standard priority"])
    draw.line(high_points, fill=BLUE, width=6, joint="curve")
    for p in high_points:
        draw.ellipse((p[0] - 5, p[1] - 5, p[0] + 5, p[1] + 5), fill="#FAFBFD", outline=BLUE, width=3)

    # Dashed line retains the distinction in grayscale.
    for start, end in zip(standard_points, standard_points[1:]):
        length = ((end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2) ** 0.5
        segments = max(1, int(length / 18))
        for j in range(0, segments, 2):
            a = j / segments
            b = min(1, (j + 1) / segments)
            draw.line(
                (
                    start[0] + (end[0] - start[0]) * a,
                    start[1] + (end[1] - start[1]) * a,
                    start[0] + (end[0] - start[0]) * b,
                    start[1] + (end[1] - start[1]) * b,
                ),
                fill=ORANGE,
                width=6,
            )
    for p in standard_points:
        draw.rectangle((p[0] - 5, p[1] - 5, p[0] + 5, p[1] + 5), fill="#FAFBFD", outline=ORANGE, width=3)

    end_x = RIGHT - 5
    draw.text((end_x - 265, standard_points[-1][1] - 40), "Standard: 28,800", font=font(20, True), fill=ORANGE)
    draw.text((end_x - 190, high_points[-1][1] - 32), "High: 0", font=font(20, True), fill=BLUE)

    draw.text(
        (LEFT, 815),
        "Source: live local UrbanPulse Kafka run, 18 Jul 2026 UTC • High avg 54.9 / max 101; Standard avg 16,653.1 / max 28,800",
        font=font(18),
        fill=MUTED,
    )
    image.save(OUTPUT, quality=95)
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()

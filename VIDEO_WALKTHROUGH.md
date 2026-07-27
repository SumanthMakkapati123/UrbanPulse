# UrbanPulse video walkthrough

Target length: 9-12 minutes. Record at 1080p with terminal text large enough to read. Do not claim measured performance without showing the evidence.

Use the macOS or Windows PowerShell commands from `PLATFORM_SETUP.md` consistently throughout the recording. Do not mix path separators or shell syntax on screen.

## 0:00-1:00 - Problem and architecture

Open the architecture figure. Explain the four rates, the sub-two-minute/90-second operational targets, and the separate weekly/monthly reporting mandate. State why the design chooses Lambda and name all four storage technologies.

## 1:00-2:00 - Kafka cluster and lifecycle

Show `docker compose ps`, the KRaft quorum status, and topic descriptions. Point out three brokers, RF=3, min ISR=2, 12/6/3/12 partitions, and the 24-hour/90-day/365-day retentions.

## 2:00-3:15 - Producers and DLQ

Show the producer configuration and `route_id` key. Run the AQI producer with 5% null injection. Display a DLQ envelope and the five-minute `evidence/dlq-report.csv`, explaining at-least-once retries and idempotence.

## 3:15-4:30 - Priority consumers

Run the priority demo and display `evidence/consumer-lag.csv` or a chart. Explain that one high-priority consumer owns all six partitions, while three standard consumers split them and intentionally process slowly. Call out the measured high versus standard lag.

## 4:30-5:15 - Route enrichment

Show the CSV/reference topic and one enriched event containing `scheduled_arrival_time`, `route_name`, and `terminal`. State clearly that the Python application implements KTable semantics but is not the JVM Kafka Streams API.

## 5:15-7:00 - PyFlink incidents

Open the Flink UI and show the running job/checkpoints. Publish deterministic fixtures, then display all three incident types. Explain sensor-keyed AQI state, junction-keyed consecutive state, and route-keyed bus pair state with the five-minute event-time timer and Haversine distance.

## 7:00-8:30 - PySpark analytics

Show the 45-minute watermark and 15-minute tumbling window in `ward_energy_stream.py`. Display a summary Kafka event and the `ward_id/date` Parquet tree. Then show the 10-minute/1-minute sliding SQL window, static zone join, `>150` filter, and an advisory event.

## 8:30-9:30 - Flink vs Spark and close

Summarise why Flink fits bus-bunching timers and Spark fits ward aggregates/Parquet. Show the evidence folder, unit-test result, Git repository URL, and final report. End with the data-sovereignty, RPO <15 minute, RTO <30 minute, open-source, and ward-accessibility controls.

## Recording checklist

- Cover contains correct student details, Git URL, and video URL.
- Official eLearn `route_schedule.csv` replaces the development fixture.
- Cluster status and topic retention are readable.
- The lag claim is backed by captured values.
- DLQ report window is five minutes.
- All three Flink incidents appear.
- Kafka and Parquet Spark outputs appear.
- No passwords, tokens, personal paths, or unrelated notifications are visible.
- Narration explains design logic instead of only reading code.

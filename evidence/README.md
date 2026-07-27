# Evidence folder

Runtime evidence belongs here. Do not fabricate values. The final repository should contain small text/CSV/PNG evidence only, not Kafka data volumes or Spark checkpoints.

Expected artifacts:

- `cluster-verification.txt`: quorum and topic descriptions
- `consumer-lag.csv`: five-second samples for both priority groups
- `priority-consumer-lag.png`: reproducible visual generated from the lag CSV
- `kafka-smoke-test.md`: reconciled producer, DLQ, join, and lag smoke-test facts
- `dlq-report.csv`: exactly five minutes of DLQ error distribution
- `enriched-event.json`: verified route-schedule join sample
- `incidents.jsonl`: AQI, gridlock, and bus-bunching examples
- `ward-energy-sample.json`: Spark Kafka summary sample
- `parquet-partitions.txt`: `ward_id/date` directory tree
- `health-advisory-sample.json`: rolling AQI advisory sample
- screenshots used in the final report/video, with descriptive names

Kafka, PyFlink, and PySpark evidence dated 18 July 2026 is included. The only
remaining visual evidence is the set of screenshots captured during the final
walkthrough recording.

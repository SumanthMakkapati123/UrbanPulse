# Kafka live smoke test

Run date: 18 July 2026 (UTC)

- Three KRaft voters healthy; broker version 4.1.2.
- All 10 topics created with replication factor 3 and minimum ISR 2.
- Bus producer: 100 events published and 100 consumed.
- AQI validation routing: 100 generated events became 71 valid AQI records and
  29 DLQ records; the two output counts reconcile exactly to the input count.
- Route enrichment: matched output for `R104` included route name, terminal,
  scheduled arrival time, and `schedule_match=true`; see `enriched-event.json`.
- Priority load: high group average/max lag 54.9/101 and final lag 0; standard
  group average/max lag 16,653.1/28,800. See `consumer-lag.csv` and the PNG chart.
- PyFlink: all three deterministic alerts emitted. AQI latency was 1,928 ms,
  gridlock was immediate, and bus bunching was 69,866 ms after its five-minute
  timer condition. See `incidents.jsonl`.

These values are local simulated-system evidence, not official MetroConnect data.
Spark evidence is tracked separately and is not claimed by this file.

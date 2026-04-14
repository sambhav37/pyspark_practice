# Spark Streaming with Wikipedia Edit Events

This example shows how to use Kafka and PySpark Structured Streaming to process simulated Wikipedia edit events.

## Setup

1. Start the full stack using the main compose file:

```bash
cd setup
docker compose up -d
```

## Produce sample events

From the `spark-jupyter` container, run:

```bash
docker exec -it spark_dev bash
cd /home/jovyan/projects/streaming
python kafka_producer.py --topic wikipedia-edits --count 200 --delay 0.1
```

## Run the PySpark streaming app

From the `spark-jupyter` container, run:

```bash
docker exec -it spark_dev bash
cd /home/jovyan/projects/streaming
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  wikipedia_streaming.py
```

If you want to run `spark-submit` from your host against the local Kafka listener, use:

```bash
cd projects/streaming
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  wikipedia_streaming.py
```

## Notes

- `checkpointLocation` is set to `checkpoints/wikipedia_edits_checkpoint` to preserve progress and state across restarts.
- The streaming query uses `withWatermark("event_time", "2 minutes")` to manage state for late data and reduce memory usage.
- The query is stateful because it aggregates page counts across the incoming event stream.
- Output uses `complete` mode to show the current top 5 pages by edit count (sorted descending). Complete mode displays the full current state each time it updates.
- Kafka is reachable inside Docker as `kafka:9092` and from the host as `localhost:29092`.

## Check Kafka events

To see the number of events in the Kafka topic:

```bash
# Latest offsets (current message count)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic wikipedia-edits --time -1

# Earliest offsets
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic wikipedia-edits --time -2
```

Subtract earliest from latest to get total messages per partition.

## Processing historical events

The app uses `startingOffsets: "latest"`, so it only processes new events after startup.

If events were produced before the streaming job started:

- **To process all historical events on next run**: Delete the checkpoint directory and change `startingOffsets` to `"earliest"` in the code.

- **To continue from last processed**: Keep the checkpoint (default behavior).

Example to reprocess from beginning:

```bash
# Stop the job, delete checkpoint
rm -rf projects/streaming/checkpoints/

# Edit wikipedia_streaming.py: change "latest" to "earliest"
# Then restart the job
```

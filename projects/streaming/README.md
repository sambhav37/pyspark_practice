# Spark Streaming with Wikipedia Edit Events

This example shows how to use Kafka and PySpark Structured Streaming to process simulated Wikipedia edit events.

## Setup

1. Start the full stack using the main compose file:

```bash
cd setup
docker compose up -d
```

2. Configure Spark properties in `properties/spark.properties` (already set for Minio S3)

## Produce sample events

From the `spark-jupyter` container, run:

```bash
docker exec -it spark_dev bash
cd /home/jovyan/projects/streaming
python kafka_producer.py --topic wikipedia-edits --count 200 --delay 0.1
```

## Produce real Wikimedia events

From the `spark-jupyter` container, run:

```bash
docker exec -it spark_dev bash
cd /home/jovyan/projects/streaming
pip install -r requirements.txt  # Install additional dependencies
python wikimedia_producer.py --topic wikimedia-edits --max-events 1000
```

This connects to the live Wikimedia EventStream API and sends real edit events to Kafka. The producer includes automatic reconnection logic to handle network interruptions.

## Run the PySpark streaming app

From the `spark-jupyter` container, run:

```bash
docker exec -it spark_dev bash
cd /home/jovyan/projects/streaming
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  wikipedia_streaming.py
```

## Run the Wikimedia streaming app

From the `spark-jupyter` container, run:

```bash
docker exec -it spark_dev bash
cd /home/jovyan/projects/streaming
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  wikimedia_streaming.py
```

This processes real Wikimedia edit events with 5-minute windows.

If you want to run `spark-submit` from your host against the local Kafka listener, use:

```bash
cd projects/streaming
KAFKA_BOOTSTRAP_SERVERS=localhost:29092 spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  wikipedia_streaming.py
```

## Storage and viewing streaming outputs

The simulated app writes windowed aggregated results to Minio S3 in Parquet format at `s3a://sampra/output/streaming/page_counts`.

The Wikimedia app writes results to `s3a://sampra/output/streaming/wikimedia_page_counts`.

Both use tumbling windows with append mode - writes completed windows to Parquet files.

### Common storage options for streaming outputs:

- **File systems**: Parquet on S3, HDFS, local FS - for scalable analytics
- **Databases**: JDBC (PostgreSQL, MySQL), Cassandra, MongoDB - for transactional storage
- **Message queues**: Kafka - for event-driven processing
- **In-memory**: For testing/debugging

### Viewing stored data:

**From Spark (inside container):**
```bash
# View simulated data
docker exec -it spark_dev bash
cd /home/jovyan/projects/streaming
spark-submit --master local[*] -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ViewData').getOrCreate()
df = spark.read.parquet('s3a://sampra/output/streaming/page_counts')
df.orderBy('window.start', 'count', ascending=[False, False]).show(10)
"

# View real Wikimedia data
spark-submit --master local[*] -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ViewWikimediaData').getOrCreate()
df = spark.read.parquet('s3a://sampra/output/streaming/wikimedia_page_counts')
df.orderBy('window_start', 'edit_count', ascending=[False, False]).show(10)
"
```

**From Minio UI:**
- Open http://localhost:9001 (admin/password)
- Navigate to sampra/output/streaming/page_counts (simulated data)
- Navigate to sampra/output/streaming/wikimedia_page_counts (real data)
- Download Parquet files and view with Parquet tools

**For production viewing:**
- Use Presto/Trino for SQL queries on S3 Parquet
- Load into BI tools via S3 connectors
- Build dashboards reading Parquet files

## Notes

- **Simulated app**: Uses 1-minute tumbling windows, processes simulated Wikipedia edits
- **Wikimedia app**: Uses 5-minute tumbling windows, processes real Wikipedia edits from EventStream API
- Both apps use `checkpointLocation` to preserve progress and state across restarts
- Streaming queries use `withWatermark` to manage state for late data and reduce memory usage
- Both queries are stateful because they aggregate edit counts across the incoming event stream
- Output uses `append` mode with Parquet on S3 - appends new completed windows to files
- Configuration loaded from `properties/spark.properties` - modify for different environments
- Kafka is reachable inside Docker as `kafka:9092` and from the host as `localhost:29092`

## Check Kafka events

To see the number of events in the Kafka topic:

```bash
# Latest offsets (current message count)
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic wikipedia-edits --time -1
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic wikimedia-edits --time -1

# Earliest offsets
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic wikipedia-edits --time -2
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic wikimedia-edits --time -2
```

Subtract earliest from latest to get total messages per partition.

## Processing historical events

Both apps use `startingOffsets: "latest"`, so they only process new events after startup.

If events were produced before the streaming job started:

- **To process all historical events on next run**: Delete the checkpoint directory and change `startingOffsets` to `"earliest"` in the code.

- **To continue from last processed**: Keep the checkpoint (default behavior).

Example to reprocess from beginning:

```bash
# Stop the job, delete checkpoint
rm -rf projects/streaming/checkpoints/

# Edit the streaming file: change "latest" to "earliest"
# Then restart the job
```
kafka-topics --create --topic wikimedia-edits --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
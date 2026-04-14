#!/usr/bin/env python3
"""
Wikipedia Recent Changes Producer

Reads from the Wikimedia EventStream API and sends events to Kafka.
Uses Server-Sent Events (SSE) to consume the real-time stream.
"""

import json
import time
import logging
from datetime import datetime
import requests
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WikimediaProducer:
    def __init__(self, kafka_bootstrap_servers='kafka:9092', topic='wikimedia-edits'):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic = topic
        self.producer = None

    def create_producer(self):
        """Create Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k is not None else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Connected to Kafka at {self.kafka_bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise

    def connect_to_stream(self):
        """Connect to Wikimedia EventStream"""
        url = "https://stream.wikimedia.org/v2/stream/recentchange"
        headers = {
            'User-Agent': 'Wikipedia-Streaming-Example/1.0 (https://github.com/example)',
            'Accept': 'text/event-stream',
            'Cache-Control': 'no-cache',
        }

        try:
            logger.info(f"Connecting to Wikimedia stream: {url}")
            response = requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=30  # Add timeout
            )
            response.raise_for_status()
            logger.info("Successfully connected to Wikimedia stream")
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to connect to Wikimedia stream: {e}")
            raise

    def parse_sse_event(self, lines):
        """Parse SSE event from lines"""
        event = {'event': 'message', 'data': '', 'id': None}
        for line_bytes in lines:
            try:
                line = line_bytes.decode('utf-8', errors='replace').strip()
            except UnicodeDecodeError:
                continue

            if line.startswith('event:'):
                event['event'] = line[6:].strip()
            elif line.startswith('id:'):
                event['id'] = line[3:].strip()
            elif line.startswith('data:'):
                event['data'] += line[5:].strip() + '\n'
            elif line.startswith(':'):
                # Comment line, ignore
                continue

        if event['data']:
            event['data'] = event['data'].strip()
            return event
        return None

    def process_event(self, event):
        """Process a single Wikimedia event and extract relevant fields"""
        try:
            data = json.loads(event['data'])

            # Extract relevant fields for our streaming example
            processed_event = {
                'event_id': data.get('id'),
                'event_type': data.get('type', 'unknown'),
                'page_title': data.get('title', ''),
                'user': data.get('user', ''),
                'timestamp': data.get('timestamp'),
                'event_time': datetime.fromtimestamp(data.get('timestamp', 0)).isoformat(),
                'namespace': data.get('namespace', 0),
                'comment': data.get('comment', ''),
                'bot': data.get('bot', False),
                'minor': data.get('minor', False),
                'wiki': data.get('wiki', ''),
                'server_name': data.get('server_name', ''),
                'length_old': data.get('length', {}).get('old'),
                'length_new': data.get('length', {}).get('new'),
                'edit_size': None  # Calculate if lengths are available
            }

            # Calculate edit size if both lengths are available
            if processed_event['length_old'] is not None and processed_event['length_new'] is not None:
                processed_event['edit_size'] = processed_event['length_new'] - processed_event['length_old']

            return processed_event

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse event data: {e}")
            return None
    def send_to_kafka(self, event):
        """Send processed event to Kafka"""
        try:
            key = event.get('event_id')
            if key is not None:
                key = str(key)
            self.producer.send(self.topic, value=event, key=key)
            logger.debug(f"Sent event to Kafka: {key}")
        except Exception as e:
            logger.error(f"Failed to send event to Kafka: {e}")
            raise

    def run(self, max_events=None, delay_between_events=0):
        """Main producer loop with reconnection logic"""
        self.create_producer()

        event_count = 0
        retry_count = 0
        max_retries = 5

        while True:
            try:
                logger.info("Attempting to connect to Wikimedia stream...")
                response = self.connect_to_stream()
                retry_count = 0  # Reset retry count on successful connection

                buffer = b""
                current_event_lines = []

                # Read raw data from the stream
                chunk_count = 0
                for chunk in response.iter_content(chunk_size=8192, decode_unicode=False):
                    if not chunk:
                        continue

                    chunk_count += 1
                    if chunk_count % 100 == 0:  # Log every 100 chunks
                        logger.info(f"Received {chunk_count} chunks from stream")

                    buffer += chunk
                    # Split on newlines
                    lines = buffer.split(b'\n')

                    # Keep the last incomplete line in buffer
                    buffer = lines.pop() if lines and not lines[-1].endswith(b'\n') else b""

                    for line_bytes in lines:
                        try:
                            line = line_bytes.decode('utf-8', errors='replace').strip()
                        except UnicodeDecodeError:
                            # Skip lines that can't be decoded
                            continue

                        if line:
                            current_event_lines.append(line_bytes)
                            if len(current_event_lines) <= 5:  # Log first few lines for debugging
                                logger.info(f"Line {len(current_event_lines)}: {line[:100]}...")
                        else:
                            # Empty line indicates end of event
                            logger.info(f"Empty line found, parsing event with {len(current_event_lines)} lines")
                            if current_event_lines:
                                logger.debug(f"Attempting to parse event with {len(current_event_lines)} lines")
                                event = self.parse_sse_event(current_event_lines)
                                if event:
                                    logger.info(f"Parsed event: {event['event']}, data length: {len(event['data'])}")
                                    if event['event'] == 'message' and event['data']:
                                        processed_event = self.process_event(event)

                                        if processed_event:
                                            # Send all processed events to Kafka
                                            self.send_to_kafka(processed_event)
                                            event_count += 1
                                            logger.info(f"Processed event {event_count}: {processed_event['page_title']}")

                                            if max_events and event_count >= max_events:
                                                logger.info(f"Reached max events limit: {max_events}")
                                                response.close()
                                                return

                                            if delay_between_events > 0:
                                                time.sleep(delay_between_events)
                                        else:
                                            logger.debug("Event processing returned None")
                                    else:
                                        logger.debug(f"Event not a message or no data: {event}")
                                else:
                                    logger.debug("Failed to parse event")

                                current_event_lines = []

                            # Log progress every 10 events
                            if event_count > 0 and event_count % 10 == 0:
                                logger.info(f"Processed {event_count} events so far")

                    # Flush producer periodically
                    if event_count % 100 == 0 and event_count > 0:
                        self.producer.flush()
                        logger.info(f"Processed {event_count} events")

            except (requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                logger.warning(f"Connection error: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Max retries ({max_retries}) exceeded")
                    break
                wait_time = min(30, 2 ** retry_count)  # Exponential backoff, max 30s
                logger.info(f"Retrying in {wait_time} seconds (attempt {retry_count}/{max_retries})")
                time.sleep(wait_time)
                continue
            except KeyboardInterrupt:
                logger.info("Producer stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                retry_count += 1
                if retry_count >= max_retries:
                    logger.error(f"Max retries ({max_retries}) exceeded")
                    break
                time.sleep(5)
                continue
            finally:
                try:
                    response.close()
                except:
                    pass

        # Final cleanup
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Producer closed. Total events processed: {event_count}")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Wikipedia Recent Changes Producer')
    parser.add_argument('--kafka-servers', default='kafka:9092',
                       help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='wikimedia-edits',
                       help='Kafka topic name')
    parser.add_argument('--max-events', type=int, default=None,
                       help='Maximum number of events to process (None for unlimited)')
    parser.add_argument('--delay', type=float, default=0,
                       help='Delay between events in seconds')

    args = parser.parse_args()

    producer = WikimediaProducer(
        kafka_bootstrap_servers=args.kafka_servers,
        topic=args.topic
    )

    logger.info(f"Starting Wikimedia producer -> Kafka topic: {args.topic}")
    producer.run(max_events=args.max_events, delay_between_events=args.delay)

if __name__ == '__main__':
    main()
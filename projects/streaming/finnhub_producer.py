#!/usr/bin/env python3
"""
Finnhub Real-Time Stock Data Producer

Connects to Finnhub WebSocket API and streams real-time stock data to Kafka.
Uses WebSocket connection for real-time financial data.
"""

# python finnhub_producer.py --api-key d7l1unhr01qm7o0a75ugd7l1unhr01qm7o0a75v0 --symbols AAPL MSFT GOOGL
# python finnhub_producer.py --api-key d7l1unhr01qm7o0a75ugd7l1unhr01qm7o0a75v0 --max-messages 100 --delay 0.1 --symbols AAPL MSFT GOOGL

import json
import asyncio
import logging
import time
from datetime import datetime
import websockets
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinnhubProducer:
    def __init__(self, api_key, kafka_bootstrap_servers='kafka:9092', topic='finnhub-stocks'):
        self.api_key = api_key
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic = topic
        self.producer = None
        self.websocket_url = f"wss://ws.finnhub.io?token={api_key}"

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

    async def subscribe_to_symbols(self, websocket, symbols):
        """Subscribe to stock symbols"""
        for symbol in symbols:
            subscribe_message = {
                "type": "subscribe",
                "symbol": symbol
            }
            await websocket.send(json.dumps(subscribe_message))
            logger.info(f"Subscribed to {symbol}")

    def process_trade(self, data):
        """Process a trade message from Finnhub"""
        try:
            # Finnhub trade data structure
            processed_trade = {
                'symbol': data.get('s'),
                'price': data.get('p'),
                'volume': data.get('v'),
                'timestamp': data.get('t'),
                'event_time': datetime.fromtimestamp(data.get('t', 0) / 1000).isoformat(),
                'conditions': data.get('c', []),
                'trade_id': data.get('i'),
                'exchange': data.get('x'),
                'trade_type': 'trade'
            }
            return processed_trade
        except Exception as e:
            logger.warning(f"Failed to process trade data: {e}")
            return None

    def send_to_kafka(self, data):
        """Send processed data to Kafka"""
        try:
            key = data.get('symbol')
            self.producer.send(self.topic, value=data, key=key)
            logger.debug(f"Sent trade to Kafka: {key}")
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {e}")
            raise

    async def run_websocket(self, symbols, max_messages=None, delay=0):
        """Main WebSocket connection and message processing loop"""
        event_count = 0

        try:
            async with websockets.connect(self.websocket_url) as websocket:
                logger.info("Connected to Finnhub WebSocket")

                # Subscribe to symbols
                await self.subscribe_to_symbols(websocket, symbols)

                async for message in websocket:
                    try:
                        data = json.loads(message)

                        if data.get('type') == 'trade':
                            for trade in data.get('data', []):
                                processed_trade = self.process_trade(trade)
                                if processed_trade:
                                    self.send_to_kafka(processed_trade)
                                    event_count += 1
                                    logger.info(f"Processed trade {event_count}: {processed_trade['symbol']} @ {processed_trade['price']}")

                                    if delay > 0:
                                        time.sleep(delay)

                                    if max_messages and event_count >= max_messages:
                                        logger.info(f"Reached max messages limit: {max_messages}")
                                        return

                        elif data.get('type') == 'ping':
                            # Respond to ping
                            pong_message = {"type": "pong"}
                            await websocket.send(json.dumps(pong_message))
                            logger.debug("Responded to ping")

                        elif data.get('type') == 'error':
                            logger.error(f"Finnhub error: {data}")

                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse message: {e}")
                        continue

        except websockets.exceptions.ConnectionClosed:
            logger.warning("WebSocket connection closed")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            raise

    def run(self, symbols, max_messages=None, delay=0):
        """Main producer loop"""
        self.create_producer()

        try:
            # Run the async WebSocket loop
            asyncio.run(self.run_websocket(symbols, max_messages, delay))
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        finally:
            # Final cleanup
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("Producer closed.")

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Finnhub Real-Time Stock Data Producer')
    parser.add_argument('--api-key', required=True, help='Finnhub API key')
    parser.add_argument('--kafka-servers', default='kafka:9092', help='Kafka bootstrap servers')
    parser.add_argument('--topic', default='finnhub-stocks', help='Kafka topic name')
    parser.add_argument('--symbols', nargs='+', default=['AAPL', 'GOOGL', 'MSFT'], help='Stock symbols to subscribe to')
    parser.add_argument('--max-messages', type=int, default=None, help='Maximum number of messages to process (None for unlimited)')
    parser.add_argument('--delay', type=float, default=0, help='Delay between messages in seconds')

    args = parser.parse_args()

    producer = FinnhubProducer(
        api_key=args.api_key,
        kafka_bootstrap_servers=args.kafka_servers,
        topic=args.topic
    )

    logger.info(f"Starting Finnhub producer -> Kafka topic: {args.topic}")
    logger.info(f"Subscribing to symbols: {args.symbols}")
    producer.run(symbols=args.symbols, max_messages=args.max_messages, delay=args.delay)

if __name__ == '__main__':
    main()
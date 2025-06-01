# src/anquant/utils/redis_bus.py
import redis.asyncio as redis
import json
from typing import Callable, Any
from loguru import logger

class RedisEventBus:
    def __init__(self, host: str = "localhost", port: int = 6379):
        self.redis = redis.Redis(host=host, port=port, decode_responses=True)
        self.pubsub = self.redis.pubsub()

    async def publish(self, topic: str, data: Any):
        try:
            await self.redis.publish(topic, json.dumps(data, default=str))
            logger.debug(f"Published to {topic}")
        except Exception as e:
            logger.error(f"Failed to publish to {topic}: {e}")

    async def subscribe(self, topic: str, callback: Callable):
        try:
            await self.pubsub.subscribe(topic)
            logger.debug(f"Subscribed to {topic}")
            async for message in self.pubsub.listen():
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        await callback(data)
                    except Exception as e:
                        logger.error(f"Error processing {topic} message: {e}")
        except Exception as e:
            logger.error(f"Subscription to {topic} failed: {e}")

    async def close(self):
        await self.redis.close()
        logger.info("Redis connection closed")

event_bus = RedisEventBus()  # Localhost for Phase 1
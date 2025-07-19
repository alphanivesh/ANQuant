from typing import Dict, Any, List, Optional
import redis.asyncio as redis
from src.anquant.py.util.logging import setup_logging
import json
import asyncio

logger = setup_logging("redis_client", log_type="messaging")

class RedisClient:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.redis = redis.Redis(
            host=config.get('host', 'localhost'),
            port=config.get('port', 6379),
            password=config.get('password', None),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
            health_check_interval=30
        )
        logger.debug("Initialized RedisClient")

    async def test_connection(self):
        """Test Redis connection."""
        try:
            logger.debug("[RedisClient] Testing connection...")
            await asyncio.wait_for(self.redis.ping(), timeout=5.0)
            logger.debug("Redis connection test successful")
            return True
        except asyncio.TimeoutError:
            logger.error("Redis connection timeout")
            return False
        except Exception as e:
            logger.error(f"Redis connection test failed: {str(e)}")
            return False

    async def publish(self, channel: str, message: str):
        try:
            logger.debug(f"[RedisClient] [TRACE] Starting publish for channel={channel}")
            logger.debug(f"[RedisClient] [TRACE] Redis client state: {self.redis}")
            logger.debug(f"[RedisClient] [TRACE] About to call redis.publish")
            
            # Direct publish without connection test
            logger.debug(f"[RedisClient] [TRACE] Calling redis.publish directly")
            result = await self.redis.publish(channel, message)
            logger.debug(f"[RedisClient] [TRACE] Redis publish completed, result={result}")
            logger.debug(f"[RedisClient] Successfully published to channel={channel}")
        except Exception as e:
            logger.error(f"[RedisClient] Exception in publish for channel={channel}: {str(e)}", exc_info=True)
            raise

    async def cache(self, key: str, value: List[Dict], ttl: int):
        """Cache data in Redis with a specified TTL (in seconds)."""
        try:
            logger.debug(f"[RedisClient] Preparing to serialize value for key={key}, value={value}")
            value_str = json.dumps(value)
            logger.debug(f"[RedisClient] Serialization complete for key={key}, value_str={value_str}")
            await self.redis.setex(key, ttl, value_str)
            logger.debug(f"[RedisClient] Successfully cached data for key={key} with TTL={ttl}s")
        except Exception as e:
            logger.error(f"[RedisClient] Exception in cache for key={key}: {str(e)}", exc_info=True)
            raise

    async def get(self, key: str) -> List[Dict]:
        """Retrieve cached data from Redis."""
        try:
            data = await self.redis.get(key)
            if data:
                return json.loads(data)
            logger.debug(f"No data found for key {key}")
            return []
        except Exception as e:
            logger.error(f"Failed to retrieve data for key {key}: {str(e)}")
            raise

    async def close(self):
        await self.redis.close()
        logger.debug("Closed Redis connection")

    async def connect(self):
        # Dummy method for compatibility
        pass
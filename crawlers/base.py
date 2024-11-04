# crawlers/base.py
from abc import ABC, abstractmethod
import aiohttp
import logging
from typing import List, Dict, Any
import motor.motor_asyncio

logger = logging.getLogger(__name__)

class MuseumCrawler(ABC):
    def __init__(self, db_client: motor.motor_asyncio.AsyncIOMotorClient):
        self.db = db_client.museum_collections
        self.session = None

    async def init_session(self):
        """Initialize aiohttp session"""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None

    @abstractmethod
    async def get_artwork_ids(self) -> List[str]:
        """Get list of artwork IDs to crawl"""
        pass

    @abstractmethod
    async def get_artwork_data(self, artwork_id: str) -> Dict[Any, Any]:
        """Get data for a specific artwork"""
        pass

    @abstractmethod
    def transform_data(self, raw_data: Dict[Any, Any]) -> Dict[Any, Any]:
        """Transform museum-specific data to common schema"""
        pass

    async def save_artwork(self, data: Dict[Any, Any]):
        """Save artwork data to MongoDB"""
        try:
            await self.db.artworks.update_one(
                {
                    "museum.id": data["museum"]["id"],
                    "museum.originalId": data["museum"]["originalId"]
                },
                {"$set": data},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving artwork: {e}")
            raise
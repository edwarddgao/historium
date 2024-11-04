# crawlers/met.py
import logging
from datetime import datetime
from typing import Dict, Any, List
import aiohttp
import backoff

from .base import MuseumCrawler

logger = logging.getLogger(__name__)

class MetCrawler(MuseumCrawler):
    BASE_URL = "https://collectionapi.metmuseum.org/public/collection/v1/"
    CALLS_PER_SECOND = 80  # Met's rate limit

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=5)
    async def _make_request(self, url: str) -> Dict[Any, Any]:
        """Make HTTP request with retries"""
        async with self.session.get(url) as response:
            if response.status == 404:
                logger.warning(f"Artwork not found: {url}")
                return None
            response.raise_for_status()
            return await response.json()

    async def get_artwork_ids(self) -> List[str]:
        """Get list of all object IDs from Met API"""
        try:
            response = await self._make_request(f"{self.BASE_URL}objects")
            return [str(object_id) for object_id in response.get("objectIDs", [])]
        except Exception as e:
            logger.error(f"Error fetching Met artwork IDs: {e}")
            raise

    async def get_artwork_data(self, artwork_id: str) -> Dict[Any, Any]:
        """Fetch artwork data from Met API"""
        try:
            data = await self._make_request(f"{self.BASE_URL}objects/{artwork_id}")
            if data is None:
                logger.warning(f"No data returned for artwork {artwork_id}")
                return None
            return data
        except Exception as e:
            logger.error(f"Error fetching Met artwork {artwork_id}: {e}")
            raise

    def transform_data(self, raw_data: Dict[Any, Any]) -> Dict[Any, Any]:
        """Transform Met data to common schema with error handling"""
        try:
            # Return None if raw_data is None
            if raw_data is None:
                return None

            # Handle tags safely
            tags = raw_data.get("tags", [])
            if tags is None:
                tags = []

            # Handle measurements safely
            measurements = raw_data.get("measurements", [])
            if measurements is None:
                measurements = []

            # Handle constituents safely
            constituents = raw_data.get("constituents", [])
            if constituents is None:
                constituents = []

            return {
                "museum": {
                    "id": "met",
                    "name": "Metropolitan Museum of Art",
                    "originalId": str(raw_data.get("objectID", ""))
                },
                "title": {
                    "primary": raw_data.get("title", ""),
                    "alternate": [],
                    "original": ""
                },
                "dates": {
                    "created": {
                        "start": raw_data.get("objectBeginDate"),
                        "end": raw_data.get("objectEndDate"),
                        "display": raw_data.get("objectDate", ""),
                        "period": raw_data.get("period", ""),
                        "dynasty": raw_data.get("dynasty", ""),
                        "circa": False
                    },
                    "acquired": raw_data.get("accessionYear"),
                    "modified": raw_data.get("metadataDate")
                },
                "classification": {
                    "category": raw_data.get("classification", ""),
                    "medium": raw_data.get("medium", ""),
                    "department": raw_data.get("department", ""),
                    "culture": raw_data.get("culture", ""),
                },
                "creators": [{
                    "name": raw_data.get("artistDisplayName", ""),
                    "role": raw_data.get("artistRole", ""),
                    "dates": {
                        "birth": raw_data.get("artistBeginDate"),
                        "death": raw_data.get("artistEndDate")
                    },
                    "nationality": raw_data.get("artistNationality", ""),
                    "wikidata_url": raw_data.get("artistWikidata_URL", ""),
                    "ulan_url": raw_data.get("artistULAN_URL", "")
                }] if raw_data.get("artistDisplayName") else [],
                "physical": {
                    "dimensions": [{
                        "type": measurement.get("elementDescription", ""),
                        "value": list(measurement.get("elementMeasurements", {}).values())[0] if measurement.get("elementMeasurements") else None,
                        "unit": "cm",  # Met API provides measurements in cm
                    } for measurement in measurements]
                },
                "location": {
                    "museum": {
                        "gallery": raw_data.get("GalleryNumber", "")
                    },
                    "origin": {
                        "city": raw_data.get("city", ""),
                        "country": raw_data.get("country", ""),
                        "state": raw_data.get("state", ""),
                        "county": raw_data.get("county", ""),
                        "region": raw_data.get("region", ""),
                        "subregion": raw_data.get("subregion", ""),
                    }
                },
                "images": [{
                    "url": raw_data.get("primaryImage", ""),
                    "type": "primary",
                }] + [{
                    "url": url,
                    "type": "additional",
                } for url in raw_data.get("additionalImages", []) if url],
                "metadata": {
                    "isPublicDomain": raw_data.get("isPublicDomain", False),
                    "isHighlight": raw_data.get("isHighlight", False),
                    "tags": tags,
                    "rights": raw_data.get("rightsAndReproduction", ""),
                    "creditLine": raw_data.get("creditLine", ""),
                    "source": {
                        "url": raw_data.get("objectURL", ""),
                        "fetchDate": datetime.utcnow().isoformat(),
                    }
                },
                "museumSpecific": {
                    "met": {
                        "accessionNumber": raw_data.get("accessionNumber", ""),
                        "objectName": raw_data.get("objectName", ""),
                        "portfolio": raw_data.get("portfolio", ""),
                        "repository": raw_data.get("repository", ""),
                        "constituents": constituents
                    }
                }
            }
        except Exception as e:
            logger.error(f"Error transforming Met data: {e}")
            logger.error(f"Problematic raw data: {raw_data}")
            raise
# crawlers/louvre.py
import logging
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, Any, List
import aiohttp
import backoff

from .base import MuseumCrawler

logger = logging.getLogger(__name__)

class LouvreCrawler(MuseumCrawler):
    SITEMAP_URL = "https://collections.louvre.fr/sitemap.xml"
    BASE_URL = "https://collections.louvre.fr/"
    CALLS_PER_SECOND = 80  # Louvre's rate limit

    @backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=5)
    async def _make_request(self, url: str) -> str:
        """Make rate-limited HTTP request with retries"""
        async with self.session.get(url) as response:
            response.raise_for_status()
            return await response.text()

    async def get_artwork_ids(self) -> List[str]:
        """Extract artwork IDs from Louvre sitemaps"""
        logger.info("Fetching Louvre sitemap index")
        sitemap_index = await self._make_request(self.SITEMAP_URL)
        root = ET.fromstring(sitemap_index)
        
        artwork_ids = []
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Process each sitemap file
        for sitemap in root.findall('.//ns:loc', namespace):
            sitemap_url = sitemap.text
            logger.info(f"Processing sitemap: {sitemap_url}")
            
            try:
                sitemap_content = await self._make_request(sitemap_url)
                sitemap_root = ET.fromstring(sitemap_content)
                
                # Extract artwork IDs from URLs
                for url in sitemap_root.findall('.//ns:loc', namespace):
                    if '/ark:/53355/' in url.text:
                        artwork_id = url.text.split('/ark:/53355/')[-1].replace('.json', '')
                        artwork_ids.append(artwork_id)
                
            except Exception as e:
                logger.error(f"Error processing sitemap {sitemap_url}: {e}")
                
        return artwork_ids

    async def get_artwork_data(self, artwork_id: str) -> Dict[Any, Any]:
        """Fetch artwork data from Louvre API"""
        url = f"https://collections.louvre.fr/ark:/53355/{artwork_id}.json"
        try:
            json_data = await self._make_request(url)
            return json.loads(json_data)
        except Exception as e:
            logger.error(f"Error fetching artwork {artwork_id}: {e}")
            raise

    def transform_data(self, raw_data: Dict[Any, Any]) -> Dict[Any, Any]:
        """Transform Louvre data to common schema with better error handling"""
        try:
            if raw_data is None:
                return None
                
            # Safely get date created info
            date_created = raw_data.get("dateCreated", [{}])
            first_date = date_created[0] if date_created else {}
            
            # Handle potentially None values safely
            denomination_title = raw_data.get("denominationTitle", [])
            if denomination_title is None:
                denomination_title = []
                
            dimensions = raw_data.get("dimension", [])
            if dimensions is None:
                dimensions = []
                
            images = raw_data.get("image", [])
            if images is None:
                images = []
                
            bibliography = raw_data.get("bibliography", [])
            if bibliography is None:
                bibliography = []
                
            object_numbers = raw_data.get("objectNumber", [])
            if object_numbers is None:
                object_numbers = []

            return {
                "museum": {
                    "id": "louvre",
                    "name": "Musée du Louvre",
                    "originalId": raw_data.get("arkId", "")
                },
                "title": {
                    "primary": raw_data.get("title", ""),
                    "alternate": [title.get("value", "") for title in denomination_title],
                    "original": raw_data.get("titleComplement", "")
                },
                "dates": {
                    "created": {
                        "start": first_date.get("startYear"),
                        "end": first_date.get("endYear"),
                        "display": raw_data.get("displayDateCreated", ""),
                        "period": first_date.get("text", ""),
                        "circa": bool(first_date.get("imprecision"))
                    },
                    "modified": raw_data.get("modified"),
                },
                "classification": {
                    "category": raw_data.get("objectType", ""),
                    "medium": raw_data.get("materialsAndTechniques", ""),
                    "department": raw_data.get("collection", ""),
                    "culture": "",  # Not directly available in Louvre data
                },
                "physical": {
                    "dimensions": [{
                        "type": dim.get("type", ""),
                        "value": float(dim.get("value", 0)) if dim.get("value", "").replace(".", "").isdigit() else None,
                        "unit": dim.get("unit", ""),
                        "note": dim.get("note", "")
                    } for dim in dimensions],
                    "shape": raw_data.get("shape", ""),
                    "materials": raw_data.get("materialsAndTechniques", "").split(",") if raw_data.get("materialsAndTechniques") else []
                },
                "location": {
                    "museum": {
                        "room": raw_data.get("room", ""),
                        "gallery": raw_data.get("currentLocation", ""),
                    },
                    "origin": {
                        "city": raw_data.get("placeOfCreation", ""),
                        "discovery_place": raw_data.get("placeOfDiscovery", ""),
                        "discovery_date": raw_data.get("dateOfDiscovery", ""),
                        "country": "",  # Would need to parse from placeOfCreation
                    },
                    "current": raw_data.get("currentLocation", "")
                },
                "images": [{
                    "url": img.get("urlImage", ""),
                    "thumbnail_url": img.get("urlThumbnail", ""),
                    "type": img.get("type", ""),
                    "copyright": img.get("copyright", ""),
                    "position": img.get("position", 0)
                } for img in images],
                "metadata": {
                    "source": {
                        "url": raw_data.get("url", ""),
                        "fetchDate": datetime.utcnow().isoformat(),
                    }
                },
                "provenance": {
                    "acquisition": {
                        "mode": raw_data.get("acquisitionDetails", [{}])[0].get("mode", "") if raw_data.get("acquisitionDetails") else "",
                        "date": raw_data.get("acquisitionDetails", [{}])[0].get("dates", [{}])[0].get("value", "") 
                            if raw_data.get("acquisitionDetails") and raw_data.get("acquisitionDetails")[0].get("dates") else "",
                    },
                    "owners": [{
                        "name": owner.get("value", ""),
                        "note": owner.get("note", ""),
                        "role": owner.get("role", "")
                    } for owner in raw_data.get("previousOwner", []) or []],
                    "owned_by": raw_data.get("ownedBy", ""),
                    "held_by": raw_data.get("heldBy", "")
                },
                "museumSpecific": {
                    "louvre": {
                        "objectNumber": object_numbers,
                        "description": raw_data.get("description", ""),
                        "inscriptions": raw_data.get("inscriptions", ""),
                        "bibliography": bibliography,
                        "exhibitions": raw_data.get("exhibition", []),
                        "relatedWorks": raw_data.get("relatedWork", []),
                        "printsDrawingsEntity": raw_data.get("printsDrawingsEntity", ""),
                        "printsDrawingsCollection": raw_data.get("printsDrawingsCollection", ""),
                        "originalObject": raw_data.get("originalObject", ""),
                        "printState": raw_data.get("printState", ""),
                        "historicalContext": raw_data.get("historicalContext", []),
                        "objectHistory": raw_data.get("objectHistory", ""),
                        "jabachInventory": raw_data.get("jabachInventory", ""),
                        "napoleonInventory": raw_data.get("napoleonInventory", ""),
                        "isMuseesNationauxRecuperation": raw_data.get("isMuseesNationauxRecuperation", False)
                    }
                }
            }
        except Exception as e:
            logger.error(f"Error transforming Louvre data: {e}")
            logger.error(f"Problematic raw data: {raw_data}")
            raise
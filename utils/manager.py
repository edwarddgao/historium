# utils/manager.py
import asyncio
import logging
import signal
from datetime import datetime
from typing import List, Dict, Any, Set
from asyncio import Semaphore, Queue
import motor.motor_asyncio
from pymongo.errors import DuplicateKeyError
from crawlers.base import MuseumCrawler
from utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

class CrawlerManager:
    def __init__(
        self,
        mongodb_uri: str,
        max_concurrent_requests: int = 50,
        chunk_size: int = 100,
        max_artworks_per_museum: int = None,
        max_retries: int = 3
    ):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_uri)
        self.db = self.client.museum_collections
        self.max_concurrent_requests = max_concurrent_requests
        self.chunk_size = chunk_size
        self.max_artworks_per_museum = max_artworks_per_museum
        self.max_retries = max_retries
        
        # Single semaphore shared across all museums
        self.request_semaphore = Semaphore(max_concurrent_requests)
        
        # Track number of active museums for distributing workers
        self.active_museums = 0
        self.max_concurrent_per_museum = 0  # Will be calculated dynamically
        
        # Add separate queues and stats for each museum
        self.work_queues = {}
        self.stats = {}
        
        # Track active workers
        self.active_workers: Set[asyncio.Task] = set()
        self.shutdown_event = asyncio.Event()
        
        # Keep track of currently running tasks
        self.running_tasks = set()
        
        self.rate_limiter = RateLimiter()

    def _setup_signal_handlers(self):
        """Setup handlers for graceful shutdown"""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(self._shutdown(s))
            )

    async def _shutdown(self, signal):
        """Handle graceful shutdown"""
        if self.shutdown_event.is_set():  # Already shutting down
            return
            
        logger.info(f"Received exit signal {signal.name}")
        self.shutdown_event.set()
        
        # Stop all workers
        for worker in self.active_workers:
            worker.cancel()
        
        # Wait for active tasks to complete or be cancelled
        if self.running_tasks:
            logger.info(f"Waiting for {len(self.running_tasks)} tasks to complete...")
            await asyncio.gather(*self.running_tasks, return_exceptions=True)
        
        # Close MongoDB connection
        self.client.close()
        logger.info("Shutdown complete")

    async def crawl_museum(self, crawler: MuseumCrawler):
        """Crawl a single museum using worker pool"""
        try:
            self.active_museums += 1
            museum_name = crawler.__class__.__name__
            
            # Configure rate limits for this museum
            self.rate_limiter.configure(
                name=museum_name,
                calls=crawler.CALLS_PER_SECOND,
                period=1.0
            )
            
            # Calculate workers per museum based on total concurrent requests
            self.max_concurrent_per_museum = max(1, self.max_concurrent_requests // self.active_museums)
            num_workers = min(self.max_concurrent_per_museum, self.max_concurrent_requests)
            
            logger.info(f"Starting {museum_name} with {num_workers} workers")
            
            # Set up signal handlers
            self._setup_signal_handlers()
            
            # Create dedicated queue for this museum
            self.work_queues[museum_name] = Queue()
            
            # Initialize stats for this museum
            self.stats[museum_name] = {
                "total_processed": 0,
                "successful": 0,
                "failed": 0,
                "total_artworks": 0
            }
            
            await crawler.init_session()
            
            # Get artwork IDs
            artwork_ids = await crawler.get_artwork_ids()
            total_artworks = len(artwork_ids)
            self.stats[museum_name]["total_artworks"] = total_artworks
            
            if self.max_artworks_per_museum:
                artwork_ids = artwork_ids[:self.max_artworks_per_museum]
                
            logger.info(f"Found {len(artwork_ids)} artworks for {museum_name}")
            
            # Add artwork IDs to museum-specific queue
            for artwork_id in artwork_ids:
                if self.shutdown_event.is_set():
                    break
                await self.work_queues[museum_name].put(artwork_id)
            
            # Create worker pool for this museum
            workers = []
            for _ in range(num_workers):
                worker = asyncio.create_task(
                    self.worker(crawler, museum_name)
                )
                workers.append(worker)
                self.active_workers.add(worker)
            
            # Add sentinel values to stop workers
            for _ in range(num_workers):
                await self.work_queues[museum_name].put(None)
            
            # Wait for this museum's workers to finish
            await asyncio.gather(*workers, return_exceptions=True)
            
            # Log final statistics
            self._log_museum_stats(museum_name)
            
        except asyncio.CancelledError:
            logger.info(f"Crawl cancelled for {museum_name}")
            raise
        except Exception as e:
            logger.error(f"Error in {museum_name}: {e}")
            raise
        finally:
            await crawler.close_session()
            self.active_museums -= 1

    async def worker(self, crawler: MuseumCrawler, museum_name: str):
        """Worker process that handles artwork processing"""
        try:
            while not self.shutdown_event.is_set():
                try:
                    # Get next artwork ID from museum-specific queue
                    artwork_id = await asyncio.wait_for(
                        self.work_queues[museum_name].get(),
                        timeout=1.0
                    )
                    
                    # Check for sentinel value
                    if artwork_id is None:
                        break
                        
                    # Process artwork
                    try:
                        task = asyncio.create_task(
                            self.process_artwork(crawler, artwork_id)
                        )
                        self.running_tasks.add(task)
                        await task
                        self.stats[museum_name]["successful"] += 1
                    except Exception as e:
                        logger.error(f"Error processing artwork {artwork_id}: {e}")
                        self.stats[museum_name]["failed"] += 1
                    finally:
                        self.running_tasks.discard(task)
                        self.work_queues[museum_name].task_done()
                        self.stats[museum_name]["total_processed"] += 1
                        
                    # Log progress periodically
                    if self.stats[museum_name]["total_processed"] % 100 == 0:
                        self._log_progress(museum_name)
                        
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                    
        except Exception as e:
            logger.error(f"Worker error in {museum_name}: {e}")

    def _log_progress(self, museum_name: str):
        """Log current progress"""
        stats = self.stats[museum_name]
        total = stats["total_artworks"]
        processed = stats["total_processed"]
        logger.info(
            f"{museum_name}: Processed {processed}/{total} "
            f"({(processed/total*100):.2f}%) - "
            f"Success: {stats['successful']}, Failed: {stats['failed']}"
        )

    def _log_museum_stats(self, museum_name: str):
        """Log final statistics"""
        stats = self.stats[museum_name]
        logger.info(f"\n{museum_name} Final Statistics:")
        logger.info(f"  Total Processed: {stats['total_processed']}/{stats['total_artworks']}")
        logger.info(f"  Successful: {stats['successful']}")
        logger.info(f"  Failed: {stats['failed']}")
        logger.info(f"  Progress: {(stats['total_processed'] / stats['total_artworks'] * 100):.2f}%")

    async def process_artwork(self, crawler: MuseumCrawler, artwork_id: str) -> bool:
        """Process a single artwork with retries"""
        museum_name = crawler.__class__.__name__
        
        async with self.request_semaphore:
            for attempt in range(self.max_retries):
                try:
                    # Wait for rate limit
                    await self.rate_limiter.acquire(museum_name)
                    
                    raw_data = await crawler.get_artwork_data(artwork_id)
                    if raw_data is None:
                        return False
                    
                    transformed_data = crawler.transform_data(raw_data)
                    if transformed_data is None:
                        return False
                    
                    await self.save_artwork(transformed_data)
                    return True
                    
                except Exception as e:
                    if attempt == self.max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)

    async def save_artwork(self, data: Dict[Any, Any]):
        """Save artwork data to MongoDB"""
        try:
            data["_metadata"] = {
                "last_updated": datetime.utcnow().isoformat(),
                "version": "1.0"
            }
            
            await self.db.artworks.update_one(
                {
                    "museum.id": data["museum"]["id"],
                    "museum.originalId": data["museum"]["originalId"]
                },
                {"$set": data},
                upsert=True
            )
        except DuplicateKeyError:
            logger.warning(
                f"Duplicate artwork: {data['museum']['id']}/{data['museum']['originalId']}"
            )
        except Exception as e:
            logger.error(f"Error saving artwork: {e}")
            raise
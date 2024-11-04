# main.py
import asyncio
import argparse
from datetime import datetime
from pathlib import Path
from typing import List
import signal

from crawlers.louvre import LouvreCrawler
from crawlers.met import MetCrawler
from utils.manager import CrawlerManager
from utils.logging import setup_logging

async def run_crawlers(
    museums: List[str],
    mongodb_uri: str,
    max_concurrent_requests: int = 80,
    chunk_size: int = 100,
    max_artworks: int = None,
    log_level: str = "INFO",
    log_file: str = "logs/crawler.log"
):
    """Run multiple museum crawlers in parallel with graceful shutdown"""
    # Set up logging
    setup_logging(log_level, log_file)
    
    # Initialize crawler manager
    manager = CrawlerManager(
        mongodb_uri=mongodb_uri,
        max_concurrent_requests=max_concurrent_requests,
        chunk_size=chunk_size,
        max_artworks_per_museum=max_artworks
    )
    
    # Map of available crawlers
    crawler_map = {
        "louvre": LouvreCrawler,
        "met": MetCrawler
    }
    
    # Initialize selected crawlers
    crawlers = [
        crawler_map[museum](manager.client)
        for museum in museums
        if museum in crawler_map
    ]
    
    try:
        # Create tasks for each museum crawler
        tasks = [
            asyncio.create_task(manager.crawl_museum(crawler))
            for crawler in crawlers
        ]
        
        # Run all crawlers in parallel and wait for completion or shutdown
        await asyncio.gather(*tasks)
        
    except asyncio.CancelledError:
        # Handle cancellation gracefully
        pass
    finally:
        # Ensure proper cleanup
        for crawler in crawlers:
            await crawler.close_session()

def main():
    parser = argparse.ArgumentParser(description='Museum Collection Crawler')
    
    # Basic options
    parser.add_argument(
        '--museums',
        nargs='+',
        choices=['louvre', 'met'],
        default=['louvre', 'met'],
        help='Museums to crawl (default: all)'
    )
    parser.add_argument(
        '--mongodb-uri',
        default='mongodb://localhost:27017',
        help='MongoDB connection URI'
    )
    
    # Performance tuning
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=50,
        help='Maximum number of concurrent requests'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=100,
        help='Number of artworks to process in each chunk'
    )
    parser.add_argument(
        '--max-artworks',
        type=int,
        help='Maximum number of artworks to process per museum'
    )
    
    # Logging options
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )
    parser.add_argument(
        '--log-file',
        help='Log file path (logs will be written to file in addition to console)'
    )
    
    # Output options
    parser.add_argument(
        '--output-dir',
        type=Path,
        help='Directory to save additional output files'
    )

    args = parser.parse_args()
    
    # Create output directory if specified
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up log file in output directory if not specified elsewhere
        if not args.log_file:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            args.log_file = args.output_dir / f'crawler_{timestamp}.log'
    
    # Run the crawler
    try:
        asyncio.run(run_crawlers(
            museums=args.museums,
            mongodb_uri=args.mongodb_uri,
            max_concurrent_requests=args.max_concurrent,
            chunk_size=args.chunk_size,
            max_artworks=args.max_artworks,
            log_level=args.log_level,
            log_file=args.log_file
        ))
    except KeyboardInterrupt:
        print("\nShutdown requested... Please wait for active tasks to complete.")

if __name__ == "__main__":
    main()
import asyncio
import json
import logging
import os
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime
from urllib.parse import urlencode
import httpx
import feedparser

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

class PodcastIntelligencePro:
    def __init__(self):
        self.itunes_base_url = "https://itunes.apple.com"
        self.client = None
        
    async def initialize(self):
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def cleanup(self):
        if self.client:
            await self.client.aclose()
    
    async def search_itunes(self, search_query: str, country: str = "US",
                           language: Optional[str] = None, genre_id: Optional[int] = None,
                           max_results: int = 50) -> List[Dict[str, Any]]:
        try:
            params = {
                "term": search_query,
                "country": country,
                "media": "podcast",
                "limit": min(max_results, 200),
                "entity": "podcast"
            }
            
            if language:
                params["lang"] = language
            
            url = f"{self.itunes_base_url}/search?{urlencode(params)}"
            response = await self.client.get(url)
            response.raise_for_status()
            data = response.json()
            
            podcasts = []
            for result in data.get("results", []):
                if result.get("kind") == "podcast":
                    podcast = {
                        "itunes_id": result.get("collectionId"),
                        "title": result.get("collectionName"),
                        "artist": result.get("artistName"),
                        "description": result.get("description", ""),
                        "artwork_url": result.get("artworkUrl600"),
                        "feed_url": result.get("feedUrl"),
                        "country": country,
                        "primary_genre": result.get("primaryGenreName"),
                        "track_count": result.get("trackCount", 0),
                        "release_date": result.get("releaseDate"),
                        "itunes_url": result.get("collectionViewUrl"),
                    }
                    podcasts.append(podcast)
            
            logger.info(f"Found {len(podcasts)} podcasts")
            return podcasts
        except Exception as e:
            logger.error(f"Error searching iTunes: {str(e)}")
            return []
    
    async def parse_rss_feed(self, feed_url: str, max_episodes: Optional[int] = None) -> Dict[str, Any]:
        try:
            response = await self.client.get(feed_url, follow_redirects=True, timeout=15)
            response.raise_for_status()
            feed = feedparser.parse(response.text)
            
            if not feed.get("feed"):
                return {}
            
            feed_data = feed["feed"]
            rss_data = {
                "title": feed_data.get("title", ""),
                "description": feed_data.get("description", ""),
                "link": feed_data.get("link", ""),
                "language": feed_data.get("language", "en"),
                "episode_count": len(feed.get("entries", []))
            }
            
            episodes = []
            for entry in feed.get("entries", [])[:max_episodes or None]:
                episode = {
                    "title": entry.get("title", ""),
                    "description": entry.get("description", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                }
                episodes.append(episode)
            
            return {"rss_data": rss_data, "episodes": episodes, "total_episodes": len(episodes)}
        except Exception as e:
            logger.error(f"Error parsing RSS: {str(e)}")
            return {"rss_data": {}, "episodes": [], "total_episodes": 0}
    
    async def enrich_podcast_data(self, podcast: Dict[str, Any], include_episodes: bool = True,
                                  max_episodes: Optional[int] = None) -> Dict[str, Any]:
        enriched = podcast.copy()
        
        if podcast.get("feed_url") and include_episodes:
            rss_result = await self.parse_rss_feed(podcast["feed_url"], max_episodes)
            enriched["rss_data"] = rss_result.get("rss_data", {})
            enriched["episodes"] = rss_result.get("episodes", [])
            enriched["total_episodes"] = rss_result.get("total_episodes", 0)
        
        enriched["extracted_at"] = datetime.utcnow().isoformat()
        return enriched
    
    async def main(self, input_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        mode = input_data.get("mode", "search")
        podcasts = []
        
        if mode == "search":
            podcasts = await self.search_itunes(
                search_query=input_data.get("searchQuery", ""),
                country=input_data.get("country", "US"),
                language=input_data.get("language"),
                genre_id=input_data.get("genreId"),
                max_results=input_data.get("maxResults", 50)
            )
        
        enriched_podcasts = []
        for podcast in podcasts:
            enriched = await self.enrich_podcast_data(
                podcast,
                include_episodes=input_data.get("includeEpisodes", True),
                max_episodes=input_data.get("maxEpisodesPerPodcast")
            )
            enriched_podcasts.append(enriched)
        
        logger.info(f"Processed {len(enriched_podcasts)} podcasts")
        return enriched_podcasts


async def run():
    input_data = {}
    
    input_file = os.getenv("APIFY_INPUT_FILE")
    if input_file and os.path.exists(input_file):
        try:
            with open(input_file, 'r') as f:
                input_data = json.load(f)
        except Exception as e:
            logger.error(f"Error reading input file: {e}")
    
    if not input_data:
        input_data = {
            "mode": "search",
            "searchQuery": "technology",
            "country": "US",
            "maxResults": 5,
            "includeEpisodes": True
        }
    
    logger.info(f"Starting with input: {json.dumps(input_data, indent=2)}")
    
    intelligence = PodcastIntelligencePro()
    await intelligence.initialize()
    
    try:
        results = await intelligence.main(input_data)
        logger.info(f"Successfully processed {len(results)} podcasts")
        
        dataset_dir = os.getenv("APIFY_DEFAULT_DATASET_PATH", "/tmp/dataset")
        os.makedirs(dataset_dir, exist_ok=True)
        
        output_file = os.path.join(dataset_dir, "results.jsonl")
        with open(output_file, 'w') as f:
            for result in results:
                f.write(json.dumps(result, default=str) + '\n')
        
        logger.info(f"Saved {len(results)} results to {output_file}")
        
        for result in results:
            print(json.dumps(result, default=str))
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        raise
    finally:
        await intelligence.cleanup()


if __name__ == "__main__":
    asyncio.run(run())

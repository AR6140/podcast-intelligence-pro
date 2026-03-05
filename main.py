import asyncio
import json
import logging
import os
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from urllib.parse import urlencode
import httpx
import feedparser
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
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
    
    async def search_itunes(self, search_query: str, country: str = "US", max_results: int = 50) -> List[Dict[str, Any]]:
        try:
            params = {"term": search_query, "country": country, "media": "podcast", "limit": min(max_results, 200), "entity": "podcast"}
            url = f"{self.itunes_base_url}/search?{urlencode(params)}"
            response = await self.client.get(url)
            response.raise_for_status()
            data = response.json()
            
            podcasts = []
            for result in data.get("results", []):
                if result.get("kind") == "podcast":
                    podcasts.append({
                        "itunes_id": result.get("collectionId"),
                        "title": result.get("collectionName"),
                        "host_name": result.get("artistName"),
                        "artist": result.get("artistName"),
                        "description": result.get("description", ""),
                        "feed_url": result.get("feedUrl"),
                        "country": country,
                        "genre": result.get("primaryGenreName"),
                        "track_count": result.get("trackCount", 0),
                        "release_date": result.get("releaseDate"),
                        "itunes_url": result.get("collectionViewUrl"),
                    })
            return podcasts
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            return []
    
    async def scrape_website(self, podcast_url: str) -> Dict[str, Any]:
        try:
            if not podcast_url:
                return {}
            response = await self.client.get(podcast_url, follow_redirects=True, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            text = soup.get_text()
            
            result = {}
            emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
            if emails:
                result["contact_email"] = emails[0]
            
            return result
        except:
            return {}
    
    async def main(self, input_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        podcasts = await self.search_itunes(
            search_query=input_data.get("searchQuery", ""),
            country=input_data.get("country", "US"),
            max_results=input_data.get("maxResults", 50)
        )
        
        results = []
        for podcast in podcasts:
            enriched = podcast.copy()
            if podcast.get("itunes_url"):
                website_data = await self.scrape_website(podcast.get("itunes_url"))
                enriched.update(website_data)
            enriched["extracted_at"] = datetime.utcnow().isoformat()
            results.append(enriched)
        
        return results

async def run():
    input_file = os.getenv("APIFY_INPUT_FILE")
    input_data = {}
    
    if input_file and os.path.exists(input_file):
        with open(input_file, 'r') as f:
            input_data = json.load(f)
    
    if not input_data:
        input_data = {"searchQuery": "technology", "country": "US", "maxResults": 50, "includeEpisodes": True}
    
    actor = PodcastIntelligencePro()
    await actor.initialize()
    
    try:
        results = await actor.main(input_data)
        for result in results:
            print(json.dumps(result, default=str))
    finally:
        await actor.cleanup()

if __name__ == "__main__":
    asyncio.run(run())

import asyncio
import json
import logging
import os
import re
import csv
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
    
    async def search_itunes(self, search_query: str, country: str = "US", max_results: int = 50) -> List[Dict]:
        try:
            params = {"term": search_query, "country": country, "media": "podcast", "limit": min(max_results, 200), "entity": "podcast"}
            response = await self.client.get(f"{self.itunes_base_url}/search?{urlencode(params)}")
            data = response.json()
            
            return [{
                "itunes_id": r.get("collectionId"),
                "title": r.get("collectionName"),
                "host_name": r.get("artistName"),
                "artist": r.get("artistName"),
                "description": r.get("description", ""),
                "feed_url": r.get("feedUrl"),
                "country": country,
                "genre": r.get("primaryGenreName"),
                "track_count": r.get("trackCount", 0),
                "release_date": r.get("releaseDate"),
                "itunes_url": r.get("collectionViewUrl"),
            } for r in data.get("results", []) if r.get("kind") == "podcast"]
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            return []
    
    async def get_contact_email(self, podcast_url: str) -> str:
        try:
            if not podcast_url:
                return ""
            response = await self.client.get(podcast_url, follow_redirects=True, timeout=10)
            text = BeautifulSoup(response.text, "html.parser").get_text()
            emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)
            return emails[0] if emails else ""
        except:
            return ""
    
    async def main(self, input_data: Dict) -> List[Dict]:
        podcasts = await self.search_itunes(
            search_query=input_data.get("searchQuery", ""),
            country=input_data.get("country", "US"),
            max_results=input_data.get("maxResults", 50)
        )
        
        results = []
        for podcast in podcasts:
            podcast["contact_email"] = await self.get_contact_email(podcast.get("itunes_url"))
            podcast["extracted_at"] = datetime.utcnow().isoformat()
            results.append(podcast)
        
        return results

def save_csv(results: List[Dict], filepath: str):
    """Save results to CSV file"""
    try:
        if not results:
            return
        
        keys = results[0].keys()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
        
        logger.info(f"CSV saved: {filepath}")
    except Exception as e:
        logger.error(f"Error saving CSV: {str(e)}")

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
        logger.info(f"Extracted {len(results)} podcasts")
        
        if results:
            kv_path = os.getenv("APIFY_DEFAULT_KEY_VALUE_STORE_PATH", "/tmp/kv")
            csv_file = os.path.join(kv_path, "podcast_results.csv")
            save_csv(results, csv_file)
    finally:
        await actor.cleanup()

if __name__ == "__main__":
    asyncio.run(run())

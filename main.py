"""
Podcast Intelligence Pro - Comprehensive podcast data extraction and enrichment
Extract iTunes/Apple Podcasts data, RSS feeds, website contact info, and advertiser intelligence
"""

import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from urllib.parse import urlencode
import httpx
import feedparser
from bs4 import BeautifulSoup
import re

from apify import Actor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PodcastIntelligencePro:
    """Main actor class for podcast intelligence extraction and enrichment"""
    
    def __init__(self):
        self.itunes_base_url = "https://itunes.apple.com"
        self.client = None
        self.debug_mode = False
        
    async def initialize(self):
        """Initialize HTTP client"""
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def cleanup(self):
        """Cleanup resources"""
        if self.client:
            await self.client.aclose()
    
    # ==================== iTunes API Methods ====================
    
    async def search_itunes(self, 
                           search_query: str, 
                           country: str = "US",
                           language: Optional[str] = None,
                           genre_id: Optional[int] = None,
                           max_results: int = 50) -> List[Dict[str, Any]]:
        """
        Search iTunes/Apple Podcasts API
        
        Args:
            search_query: Search term
            country: Country code (US, GB, CA, AU, etc.)
            language: Language code (en, es, fr, de, etc.)
            genre_id: iTunes genre ID (1303=Comedy, 1311=News, 1321=Business, 1318=Technology)
            max_results: Max podcasts to return (1-200)
        """
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
            
            if self.debug_mode:
                logger.info(f"Querying iTunes: {url}")
            
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
                        "genres": [result.get("primaryGenreName")],
                        "track_count": result.get("trackCount", 0),
                        "release_date": result.get("releaseDate"),
                        "itunes_url": result.get("collectionViewUrl"),
                        "content_advisory": result.get("contentAdvisoryRating", "clean"),
                        "explicit": result.get("trackExplicitness", "clean")
                    }
                    podcasts.append(podcast)
            
            if self.debug_mode:
                logger.info(f"Found {len(podcasts)} podcasts on iTunes")
            
            return podcasts
            
        except Exception as e:
            logger.error(f"Error searching iTunes: {str(e)}")
            return []
    
    async def lookup_itunes_ids(self, 
                               podcast_ids: List[int],
                               country: str = "US") -> List[Dict[str, Any]]:
        """
        Lookup podcasts by iTunes ID
        
        Args:
            podcast_ids: List of iTunes podcast IDs
            country: Country code
        """
        try:
            podcasts = []
            ids_str = ",".join(str(id) for id in podcast_ids)
            
            params = {
                "id": ids_str,
                "country": country,
                "media": "podcast",
                "entity": "podcast"
            }
            
            url = f"{self.itunes_base_url}/lookup?{urlencode(params)}"
            
            if self.debug_mode:
                logger.info(f"Looking up iTunes IDs: {url}")
            
            response = await self.client.get(url)
            response.raise_for_status()
            data = response.json()
            
            for result in data.get("results", [])[1:]:  # Skip first result (query info)
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
                        "genres": [result.get("primaryGenreName")],
                        "track_count": result.get("trackCount", 0),
                        "release_date": result.get("releaseDate"),
                        "itunes_url": result.get("collectionViewUrl"),
                        "content_advisory": result.get("contentAdvisoryRating", "clean"),
                    }
                    podcasts.append(podcast)
            
            return podcasts
            
        except Exception as e:
            logger.error(f"Error looking up iTunes IDs: {str(e)}")
            return []
    
    # ==================== RSS Feed Methods ====================
    
    async def parse_rss_feed(self, feed_url: str, 
                            max_episodes: Optional[int] = None) -> Dict[str, Any]:
        """
        Parse RSS feed and extract podcast + episode data
        
        Args:
            feed_url: URL of RSS feed
            max_episodes: Max episodes to extract (None = all)
        """
        try:
            if self.debug_mode:
                logger.info(f"Parsing RSS feed: {feed_url}")
            
            response = await self.client.get(feed_url, follow_redirects=True)
            response.raise_for_status()
            
            feed = feedparser.parse(response.text)
            
            if not feed.get("feed"):
                logger.warning(f"Invalid feed structure: {feed_url}")
                return {}
            
            feed_data = feed["feed"]
            
            rss_data = {
                "title": feed_data.get("title", ""),
                "description": feed_data.get("description") or feed_data.get("subtitle", ""),
                "link": feed_data.get("link", ""),
                "language": feed_data.get("language", "en"),
                "author": feed_data.get("author", "") or feed_data.get("itunes_author", ""),
                "image_url": None,
                "categories": [],
                "copyright": feed_data.get("rights", ""),
                "last_updated": feed_data.get("updated", ""),
                "episode_count": len(feed.get("entries", []))
            }
            
            # Extract image
            if feed_data.get("image"):
                rss_data["image_url"] = feed_data["image"].get("href")
            elif feed_data.get("itunes_image"):
                rss_data["image_url"] = feed_data["itunes_image"].get("href")
            
            # Extract categories
            if feed_data.get("tags"):
                rss_data["categories"] = [tag.get("term") for tag in feed_data["tags"] if tag.get("term")]
            
            # Extract episodes
            episodes = []
            for entry in feed.get("entries", [])[:max_episodes or None]:
                episode = {
                    "title": entry.get("title", ""),
                    "description": entry.get("description") or entry.get("summary", ""),
                    "link": entry.get("link", ""),
                    "published": entry.get("published", ""),
                    "published_parsed": entry.get("published_parsed"),
                    "guid": entry.get("id", ""),
                    "duration": entry.get("itunes_duration", ""),
                    "season": entry.get("itunes_season"),
                    "episode_number": entry.get("itunes_episode"),
                    "episode_type": entry.get("itunes_episodetype", "full"),
                    "explicit": entry.get("itunes_explicit", "clean")
                }
                
                # Extract enclosure (audio file)
                if entry.get("enclosures"):
                    enclosure = entry["enclosures"][0]
                    episode["enclosure"] = {
                        "url": enclosure.get("href"),
                        "type": enclosure.get("type"),
                        "length": enclosure.get("length")
                    }
                
                episodes.append(episode)
            
            return {
                "rss_data": rss_data,
                "episodes": episodes,
                "total_episodes": len(episodes)
            }
            
        except Exception as e:
            logger.error(f"Error parsing RSS feed {feed_url}: {str(e)}")
            return {"rss_data": {}, "episodes": [], "total_episodes": 0}
    
    # ==================== Website Scraping Methods ====================
    
    async def scrape_podcast_website(self, podcast_url: str) -> Dict[str, Any]:
        """
        Scrape podcast website for contact info, sponsor data, and metadata
        
        Args:
            podcast_url: URL of podcast website
        """
        try:
            if not podcast_url or not podcast_url.startswith("http"):
                return {}
            
            if self.debug_mode:
                logger.info(f"Scraping podcast website: {podcast_url}")
            
            response = await self.client.get(podcast_url, follow_redirects=True)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            scraped_data = {
                "podcast_website_url": podcast_url,
                "podcast_email": self._extract_email(soup),
                "network_email": None,
                "contact_info": self._extract_contact_info(soup),
                "sponsor_info": self._extract_sponsor_info(soup),
                "listener_count": self._extract_listener_count(soup),
                "social_links": self._extract_social_links(soup),
                "host_info": self._extract_host_info(soup)
            }
            
            return scraped_data
            
        except Exception as e:
            logger.warning(f"Error scraping website {podcast_url}: {str(e)}")
            return {}
    
    def _extract_email(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract email address from website"""
        # Look for mailto links
        mailto_links = soup.find_all("a", href=re.compile(r"^mailto:"))
        if mailto_links:
            email = mailto_links[0].get("href").replace("mailto:", "").split("?")[0]
            if email and "@" in email:
                return email
        
        # Look for email patterns in text
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        text = soup.get_text()
        emails = re.findall(email_pattern, text)
        
        if emails:
            # Prefer contact/info/sponsor emails
            for email in emails:
                if any(keyword in email.lower() for keyword in ["contact", "info", "sponsor", "hello", "hi"]):
                    return email
            return emails[0]
        
        return None
    
    def _extract_contact_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract contact information"""
        contact = {}
        
        # Look for phone numbers
        phone_pattern = r'\b(?:\+?1[-.]?)?\(?([0-9]{3})\)?[-.]?([0-9]{3})[-.]?([0-9]{4})\b'
        text = soup.get_text()
        phones = re.findall(phone_pattern, text)
        if phones:
            contact["phone"] = "".join(phones[0])
        
        # Look for contact form
        if soup.find("form", {"id": re.compile("contact|form", re.I)}):
            contact["has_contact_form"] = True
        
        return contact
    
    def _extract_sponsor_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract sponsorship and advertising information"""
        sponsor_info = {}
        
        # Look for sponsor sections
        sponsor_sections = soup.find_all(["div", "section"], {"class": re.compile("sponsor|advertis|partner", re.I)})
        
        if sponsor_sections:
            sponsor_info["has_sponsor_section"] = True
        
        # Look for sponsorship links
        sponsor_links = soup.find_all("a", href=re.compile("sponsor|advertis|partner", re.I))
        if sponsor_links:
            sponsor_info["sponsorship_page_url"] = sponsor_links[0].get("href")
        
        # Look for ad mentions
        text = soup.get_text().lower()
        if "sponsor" in text or "advertisement" in text or "advertise" in text:
            sponsor_info["actively_monetized"] = True
        
        return sponsor_info
    
    def _extract_listener_count(self, soup: BeautifulSoup) -> Optional[int]:
        """Extract listener/download count if available"""
        text = soup.get_text()
        
        # Look for patterns like "1.2M downloads", "500K listeners", etc.
        count_pattern = r'(\d+(?:[.,]\d+)?)\s*([MK])\s*(?:downloads|listeners|downloads per month|monthly downloads)'
        matches = re.findall(count_pattern, text, re.I)
        
        if matches:
            count, multiplier = matches[0]
            count = float(count.replace(",", "."))
            multiplier = multiplier.upper()
            
            if multiplier == "M":
                return int(count * 1_000_000)
            elif multiplier == "K":
                return int(count * 1_000)
        
        return None
    
    def _extract_social_links(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extract social media links"""
        social_links = {}
        
        social_domains = {
            "twitter": ["twitter.com", "x.com"],
            "instagram": ["instagram.com"],
            "facebook": ["facebook.com"],
            "linkedin": ["linkedin.com"],
            "youtube": ["youtube.com"]
        }
        
        all_links = soup.find_all("a", href=True)
        
        for link in all_links:
            href = link.get("href", "").lower()
            for platform, domains in social_domains.items():
                if any(domain in href for domain in domains):
                    if platform not in social_links:
                        social_links[platform] = link.get("href")
        
        return social_links
    
    def _extract_host_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract host/creator information"""
        host_info = {}
        
        # Look for host/creator mentions
        about_section = soup.find(["div", "section"], {"class": re.compile("about|host|creator|team", re.I)})
        
        if about_section:
            text = about_section.get_text()
            host_info["has_about_section"] = True
            
            # Look for names (simple heuristic)
            name_pattern = r'(?:Hosted by|Host|Creator|Created by)\s+([A-Z][a-z]+ (?:[A-Z][a-z]+)?)'
            names = re.findall(name_pattern, text)
            if names:
                host_info["hosts"] = names
        
        return host_info
    
    # ==================== Data Enrichment Methods ====================
    
    async def enrich_podcast_data(self, podcast: Dict[str, Any], 
                                  include_episodes: bool = True,
                                  max_episodes: Optional[int] = None) -> Dict[str, Any]:
        """
        Enrich podcast data from multiple sources
        
        Args:
            podcast: Base podcast data from iTunes
            include_episodes: Whether to include episode data
            max_episodes: Max episodes to include
        """
        enriched = podcast.copy()
        
        # Parse RSS feed if available
        if podcast.get("feed_url") and include_episodes:
            rss_result = await self.parse_rss_feed(podcast["feed_url"], max_episodes)
            enriched["rss_data"] = rss_result.get("rss_data", {})
            enriched["episodes"] = rss_result.get("episodes", [])
            enriched["total_episodes"] = rss_result.get("total_episodes", 0)
        
        # Scrape website if iTunes URL available
        if podcast.get("itunes_url"):
            website_data = await self.scrape_podcast_website(podcast.get("itunes_url"))
            enriched["website_data"] = website_data
        
        # Add metadata
        enriched["extracted_at"] = datetime.utcnow().isoformat()
        enriched["data_quality_score"] = self._calculate_quality_score(enriched)
        
        return enriched
    
    def _calculate_quality_score(self, podcast: Dict[str, Any]) -> float:
        """
        Calculate data quality score (0-100)
        Higher score = more complete data
        """
        score = 0.0
        
        # iTunes data (20 points)
        if podcast.get("title"):
            score += 5
        if podcast.get("description"):
            score += 5
        if podcast.get("artwork_url"):
            score += 5
        if podcast.get("feed_url"):
            score += 5
        
        # RSS data (20 points)
        if podcast.get("rss_data"):
            score += 10
            if podcast.get("episodes"):
                score += 10
        
        # Website data (30 points)
        if podcast.get("website_data"):
            website = podcast["website_data"]
            if website.get("podcast_email"):
                score += 10
            if website.get("sponsor_info"):
                score += 10
            if website.get("listener_count"):
                score += 10
        
        # Contact data (30 points)
        if podcast.get("contact_email"):
            score += 15
        if podcast.get("sales_contact"):
            score += 15
        
        return min(score, 100.0)
    
    # ==================== Main Actor Run ====================
    
    async def main(self, input_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Main actor execution
        
        Input:
        {
            "mode": "search" | "direct_urls" | "lookup_ids",
            "searchQuery": "string (required for search mode)",
            "country": "US",
            "language": "en",
            "genreId": 1321,
            "maxResults": 50,
            "feedUrls": ["url1", "url2"],
            "podcastIds": [123, 456],
            "includeEpisodes": true,
            "maxEpisodesPerPodcast": 10,
            "debugMode": false
        }
        """
        
        self.debug_mode = input_data.get("debugMode", False)
        
        mode = input_data.get("mode", "search")
        podcasts = []
        
        # Get podcasts based on mode
        if mode == "search":
            podcasts = await self.search_itunes(
                search_query=input_data.get("searchQuery", ""),
                country=input_data.get("country", "US"),
                language=input_data.get("language"),
                genre_id=input_data.get("genreId"),
                max_results=input_data.get("maxResults", 50)
            )
        
        elif mode == "direct_urls":
            podcasts = []
            for feed_url in input_data.get("feedUrls", []):
                rss_result = await self.parse_rss_feed(feed_url)
                if rss_result.get("rss_data"):
                    podcast = {
                        "title": rss_result["rss_data"].get("title"),
                        "description": rss_result["rss_data"].get("description"),
                        "feed_url": feed_url,
                        "rss_data": rss_result["rss_data"],
                        "episodes": rss_result.get("episodes", []),
                        "total_episodes": rss_result.get("total_episodes", 0)
                    }
                    podcasts.append(podcast)
        
        elif mode == "lookup_ids":
            podcasts = await self.lookup_itunes_ids(
                podcast_ids=input_data.get("podcastIds", []),
                country=input_data.get("country", "US")
            )
        
        # Enrich podcasts
        enriched_podcasts = []
        for podcast in podcasts:
            enriched = await self.enrich_podcast_data(
                podcast,
                include_episodes=input_data.get("includeEpisodes", True),
                max_episodes=input_data.get("maxEpisodesPerPodcast")
            )
            enriched_podcasts.append(enriched)
            
            # Push to Actor dataset
            await Actor.push_data(enriched)
        
        if self.debug_mode:
            logger.info(f"Processed {len(enriched_podcasts)} podcasts")
        
        return enriched_podcasts


async def main():
    """Apify Actor main entry point"""
    async with Actor:
        # Get input
        actor_input = await Actor.get_input() or {}
        
        if not actor_input:
            actor_input = {
                "mode": "search",
                "searchQuery": "technology",
                "country": "US",
                "maxResults": 10,
                "includeEpisodes": True
            }
        
        logger.info(f"Starting with input: {json.dumps(actor_input, indent=2)}")
        
        # Run actor
        intelligence = PodcastIntelligencePro()
        await intelligence.initialize()
        
        try:
            results = await intelligence.main(actor_input)
            logger.info(f"Successfully processed {len(results)} podcasts")
        finally:
            await intelligence.cleanup()


if __name__ == "__main__":
    asyncio.run(main())

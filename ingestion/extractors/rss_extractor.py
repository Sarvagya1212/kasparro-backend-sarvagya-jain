"""
RSS Feed Extractor

Extracts data from RSS/Atom feeds.
"""

import feedparser
from typing import List, Dict, Any, Optional
from datetime import datetime
from ingestion.base import DataSource
from models.raw_data import SourceType


class RSSExtractor(DataSource):
    """Extract data from RSS feeds"""
    
    def __init__(
        self,
        db_session,
        source_name: str,
        feed_url: str,
        checkpoint_type: str = "timestamp"
    ):
        super().__init__(
            db_session=db_session,
            source_type=SourceType.RSS,
            source_name=source_name,
            checkpoint_type=checkpoint_type
        )
        self.feed_url = feed_url
    
    async def fetch_data(self, checkpoint_value: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch entries from RSS feed
        
        Args:
            checkpoint_value: Last published date (ISO format)
            
        Returns:
            List of feed entries as dictionaries
        """
        # Fetch RSS content using async HTTP
        import httpx
        import asyncio
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(self.feed_url)
            response.raise_for_status()
            rss_content = response.text
        
        # Parse RSS in thread pool
        feed = await asyncio.to_thread(feedparser.parse, rss_content)
        
        if feed.bozo:  # Feed parsing error
            raise ValueError(f"Failed to parse RSS feed: {feed.bozo_exception}")
        
        entries = []
        checkpoint_dt = None
        
        if checkpoint_value:
            try:
                checkpoint_dt = datetime.fromisoformat(checkpoint_value.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                checkpoint_dt = None
        
        for entry in feed.entries:
            # Parse published date
            published = None
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                published = datetime(*entry.published_parsed[:6])
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                published = datetime(*entry.updated_parsed[:6])
            
            # Skip if older than checkpoint
            if checkpoint_dt and published and published <= checkpoint_dt:
                continue
            
            # Extract entry data
            entry_data = {
                'id': entry.get('id', entry.get('link', '')),
                'title': entry.get('title', ''),
                'description': entry.get('summary', entry.get('description', '')),
                'link': entry.get('link', ''),
                'author': entry.get('author', entry.get('dc:creator', '')),
                'published': published.isoformat() if published else None,
                'categories': [tag.get('term', '') for tag in entry.get('tags', [])],
                'content': entry.get('content', [{}])[0].get('value', '') if entry.get('content') else ''
            }
            
            entries.append(entry_data)
        
        return entries
    
    def extract_record_id(self, record: Dict[str, Any]) -> str:
        """Extract unique ID from RSS entry"""
        return record.get('id', record.get('link', ''))
    
    def extract_timestamp(self, record: Dict[str, Any]) -> Optional[datetime]:
        """Extract timestamp from RSS entry"""
        published_str = record.get('published')
        if published_str:
            try:
                return datetime.fromisoformat(published_str.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                pass
        return None
    
    def get_new_checkpoint(self, records: List[Dict[str, Any]]) -> Optional[str]:
        """Get latest published date as new checkpoint"""
        if not records:
            return None
        
        latest_date = None
        for record in records:
            if record.get('published'):
                try:
                    pub_date = datetime.fromisoformat(record['published'].replace('Z', '+00:00'))
                    if latest_date is None or pub_date > latest_date:
                        latest_date = pub_date
                except (ValueError, AttributeError):
                    continue
        
        return latest_date.isoformat() if latest_date else None
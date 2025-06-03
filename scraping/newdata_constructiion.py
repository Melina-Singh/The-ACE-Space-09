import requests
from bs4 import BeautifulSoup
import json
import time
import re
from urllib.parse import urljoin, urlparse
from datetime import datetime
import os
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Set
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class Article:
    """Data class to represent a news article"""
    title: str
    url: str
    content: str
    summary: str
    author: Optional[str]
    published_date: Optional[str]
    category: Optional[str]
    tags: List[str]
    scraped_at: str

class ConstructionDiveScraper:
    """
    Intelligent web scraper for Construction Dive using LLM strategy
    """
    
    def __init__(self, delay=1.0, max_pages=None):
        self.base_url = "https://www.constructiondive.com"
        self.delay = delay  # Respectful delay between requests
        self.max_pages = max_pages
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.scraped_urls: Set[str] = set()
        self.articles: List[Article] = []
        
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a webpage"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
            
    def extract_article_links_intelligent(self, soup: BeautifulSoup) -> List[str]:
        """
        Intelligently extract article links using multiple strategies
        """
        article_links = set()
        
        # Strategy 1: Look for common article link patterns
        article_selectors = [
            'a[href*="/news/"]',  # News articles
            'a[href*="/spons/"]',  # Sponsored content
            '.feed-item a',       # Feed items
            '.article-item a',    # Article items
            '.story-item a',      # Story items
            '.headline a',        # Headlines
            'h2 a',              # Header links
            'h3 a',              # Sub-header links
            '.title a',          # Title links
        ]
        
        for selector in article_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    full_url = urljoin(self.base_url, href)
                    if self.is_article_url(full_url):
                        article_links.add(full_url)
        
        # Strategy 2: Pattern matching for article URLs
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link['href']
            full_url = urljoin(self.base_url, href)
            if self.is_article_url(full_url):
                article_links.add(full_url)
                
        return list(article_links)
    
    def is_article_url(self, url: str) -> bool:
        """
        Determine if a URL is likely an article based on patterns
        """
        article_patterns = [
            r'/news/[^/]+/\d+/',     # /news/article-title/123456/
            r'/spons/[^/]+/\d+/',    # /spons/sponsored-title/123456/
        ]
        
        for pattern in article_patterns:
            if re.search(pattern, url):
                return True
        return False
    
    def find_pagination_links(self, soup: BeautifulSoup) -> List[str]:
        """
        Intelligently find pagination links
        """
        pagination_links = set()
        
        # Common pagination selectors
        pagination_selectors = [
            '.pagination a',
            '.pager a',
            '.page-numbers a',
            'a[rel="next"]',
            'a:contains("Next")',
            'a:contains(">")',
            '.next-page a',
            '.load-more a'
        ]
        
        for selector in pagination_selectors:
            try:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href:
                        full_url = urljoin(self.base_url, href)
                        pagination_links.add(full_url)
            except:
                continue
                
        # Look for numbered pagination
        page_links = soup.find_all('a', href=True)
        for link in page_links:
            href = link['href']
            text = link.get_text(strip=True)
            
            # Check if it's a page number or next link
            if (text.isdigit() or 
                text.lower() in ['next', '>', 'more', 'load more'] or
                'page=' in href or 
                '/page/' in href):
                full_url = urljoin(self.base_url, href)
                pagination_links.add(full_url)
        
        return list(pagination_links)
    
    def extract_article_content(self, url: str) -> Optional[Article]:
        """
        Extract article content using intelligent parsing
        """
        soup = self.get_page(url)
        if not soup:
            return None
            
        try:
            # Extract title
            title = self.extract_title(soup)
            
            # Extract content
            content = self.extract_content(soup)
            
            # Extract metadata
            author = self.extract_author(soup)
            published_date = self.extract_published_date(soup)
            category = self.extract_category(soup)
            tags = self.extract_tags(soup)
            
            # Generate summary (first 200 words)
            summary = self.generate_summary(content)
            
            return Article(
                title=title,
                url=url,
                content=content,
                summary=summary,
                author=author,
                published_date=published_date,
                category=category,
                tags=tags,
                scraped_at=datetime.now().isoformat()
            )
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {e}")
            return None
    
    def extract_title(self, soup: BeautifulSoup) -> str:
        """Extract article title"""
        title_selectors = [
            'h1',
            '.article-title',
            '.headline',
            '.story-title',
            'title'
        ]
        
        for selector in title_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        return "No title found"
    
    def extract_content(self, soup: BeautifulSoup) -> str:
        """Extract main article content"""
        content_selectors = [
            '.article-content',
            '.story-content',
            '.content',
            '.article-body',
            '.post-content',
            'article',
            '.main-content'
        ]
        
        for selector in content_selectors:
            element = soup.select_one(selector)
            if element:
                # Remove unwanted elements
                for unwanted in element.select('script, style, .ad, .advertisement, .social-share'):
                    unwanted.decompose()
                return element.get_text(strip=True, separator=' ')
        
        # Fallback: extract all paragraph text
        paragraphs = soup.find_all('p')
        return ' '.join([p.get_text(strip=True) for p in paragraphs])
    
    def extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article author"""
        author_selectors = [
            '.author',
            '.byline',
            '.writer',
            '[rel="author"]',
            '.article-author'
        ]
        
        for selector in author_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        return None
    
    def extract_published_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract published date"""
        date_selectors = [
            'time',
            '.date',
            '.published',
            '.article-date',
            '[datetime]'
        ]
        
        for selector in date_selectors:
            element = soup.select_one(selector)
            if element:
                # Try to get datetime attribute first
                if element.has_attr('datetime'):
                    return element['datetime']
                return element.get_text(strip=True)
        return None
    
    def extract_category(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract article category"""
        category_selectors = [
            '.category',
            '.section',
            '.tag',
            '.topic'
        ]
        
        for selector in category_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        return None
    
    def extract_tags(self, soup: BeautifulSoup) -> List[str]:
        """Extract article tags"""
        tags = []
        tag_selectors = [
            '.tags a',
            '.categories a',
            '.keywords a'
        ]
        
        for selector in tag_selectors:
            elements = soup.select(selector)
            for element in elements:
                tags.append(element.get_text(strip=True))
        
        return tags
    
    def generate_summary(self, content: str, max_words: int = 50) -> str:
        """Generate a summary from content"""
        words = content.split()
        if len(words) <= max_words:
            return content
        return ' '.join(words[:max_words]) + '...'
    
    def scrape_all_articles(self, start_url: Optional[str] = None) -> List[Article]:
        """
        Main scraping method using intelligent pagination discovery
        """
        if not start_url:
            start_url = self.base_url
            
        pages_to_visit = [start_url]
        visited_pages = set()
        page_count = 0
        
        logger.info(f"Starting scrape from: {start_url}")
        
        while pages_to_visit and (not self.max_pages or page_count < self.max_pages):
            current_url = pages_to_visit.pop(0)
            
            if current_url in visited_pages:
                continue
                
            logger.info(f"Scraping page {page_count + 1}: {current_url}")
            visited_pages.add(current_url)
            
            soup = self.get_page(current_url)
            if not soup:
                continue
            
            # Extract article links from current page
            article_links = self.extract_article_links_intelligent(soup)
            logger.info(f"Found {len(article_links)} article links on this page")
            
            # Scrape each article
            for article_url in article_links:
                if article_url not in self.scraped_urls:
                    logger.info(f"Scraping article: {article_url}")
                    article = self.extract_article_content(article_url)
                    if article:
                        self.articles.append(article)
                        self.scraped_urls.add(article_url)
                    
                    time.sleep(self.delay)  # Be respectful
            
            # Find pagination links for next pages
            pagination_links = self.find_pagination_links(soup)
            for link in pagination_links:
                if link not in visited_pages and link not in pages_to_visit:
                    pages_to_visit.append(link)
            
            page_count += 1
            time.sleep(self.delay)  # Be respectful between pages
        
        logger.info(f"Scraping completed. Found {len(self.articles)} articles across {page_count} pages")
        return self.articles
    
    def save_articles(self, filename: str = 'construction_dive_articles.json'):
        """Save scraped articles to JSON file"""
        articles_data = [asdict(article) for article in self.articles]
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(articles_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(articles_data)} articles to {filename}")
    
    def save_articles_csv(self, filename: str = 'construction_dive_articles.csv'):
        """Save scraped articles to CSV file"""
        import csv
        
        if not self.articles:
            logger.warning("No articles to save")
            return
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['Title', 'URL', 'Content', 'Summary', 'Author', 
                           'Published Date', 'Category', 'Tags', 'Scraped At'])
            
            # Write articles
            for article in self.articles:
                writer.writerow([
                    article.title,
                    article.url,
                    article.content,
                    article.summary,
                    article.author,
                    article.published_date,
                    article.category,
                    '|'.join(article.tags),  # Join tags with pipe separator
                    article.scraped_at
                ])
        
        logger.info(f"Saved {len(self.articles)} articles to {filename}")

# Usage example
def main():
    """
    Main function to run the scraper
    """
    # Initialize scraper with 1 second delay between requests
    scraper = ConstructionDiveScraper(delay=1.0, max_pages=10)  # Limit to 10 pages for testing
    
    # Start scraping from the main page
    articles = scraper.scrape_all_articles()
    
    # Save results
    scraper.save_articles('construction_dive_articles.json')
    scraper.save_articles_csv('construction_dive_articles.csv')
    
    # Print summary
    print(f"\nScraping Summary:")
    print(f"Total articles scraped: {len(articles)}")
    print(f"Sample articles:")
    
    for i, article in enumerate(articles[:3]):  # Show first 3 articles
        print(f"\n{i+1}. {article.title}")
        print(f"   URL: {article.url}")
        print(f"   Author: {article.author}")
        print(f"   Date: {article.published_date}")
        print(f"   Summary: {article.summary[:100]}...")

# Advanced usage with custom starting points
def scrape_specific_sections():
    """
    Example of scraping specific sections
    """
    scraper = ConstructionDiveScraper(delay=1.0)
    
    # You can start from specific sections if you know the URLs
    # For example, if Construction Dive has section pages like:
    # - /news/
    # - /technology/
    # - /safety/
    
    sections_to_scrape = [
        "https://www.constructiondive.com/",  # Main page
        # Add other section URLs as discovered
    ]
    
    all_articles = []
    for section_url in sections_to_scrape:
        logger.info(f"Scraping section: {section_url}")
        articles = scraper.scrape_all_articles(section_url)
        all_articles.extend(articles)
    
    return all_articles

if __name__ == "__main__":
    main()
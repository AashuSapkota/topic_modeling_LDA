#!/usr/bin/env python3
"""
Enhanced Nepali News Scraper for Academic Research
Advanced Machine Learning Assignment - Task 1
Author: Sabin Sapkota, Suresh Chaudhary, Rashik Khadka
Date: August 30, 2025


"""

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import json
import time
from datetime import datetime, timedelta
import calendar
import random
import logging
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedNepaliNewsScraper:
    """
    Enhanced web scraper for Nepali news articles with academic research quality standards.
    
    Features:
    - Robust error handling and retry mechanisms
    - Respectful rate limiting to avoid server overload
    - Data quality validation and filtering
    - Comprehensive metadata extraction
    - Academic research compliance
    """
    
    def __init__(self, delay_between_requests: float = 2.0, max_retries: int = 3):
        """
        Initialize the enhanced scraper.
        
        Args:
            delay_between_requests: Seconds to wait between requests
            max_retries: Maximum retry attempts for failed requests
        """
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.delay_between_requests = delay_between_requests
        self.delay_between_articles = 1.0
        self.max_retries = max_retries
        self.scraped_articles = []
        
        logger.info(f"Initialized scraper with {delay_between_requests}s delay and {max_retries} max retries")

    def scrape_article(self, url: str) -> Optional[Dict]:
        """
        Scrape a single article with comprehensive error handling.
        
        Args:
            url: URL of the article to scrape
            
        Returns:
            Dictionary containing article data or None if failed
        """
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract basic content
                title_tag = soup.find('h1')
                content_div = soup.find('div', class_='ok18-single-post-content-wrap')
                
                if not title_tag or not content_div:
                    logger.warning(f"Missing structure in {url}")
                    return None
                
                title = title_tag.get_text(strip=True)
                paragraphs = content_div.find_all('p')
                content = ' '.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
                
                # Data quality check
                if len(content) < 100:
                    logger.warning(f"Content too short ({len(content)} chars): {url}")
                    return None
                
                # Extract metadata
                timestamp = self._extract_timestamp(soup)
                category = self._extract_category(soup, url)
                author = self._extract_author(soup)
                
                article_data = {
                    "url": url,
                    "timestamp": timestamp,
                    "title": title,
                    "content": content,
                    "category": category,
                    "author": author,
                    "word_count": len(content.split()),
                    "char_count": len(content),
                    "scraped_at": datetime.now().isoformat(),
                    "scraper_version": "enhanced_v1.0"
                }
                
                logger.debug(f"Successfully scraped: {title[:50]}...")
                return article_data
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed for {url} (attempt {attempt+1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    sleep_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to scrape {url} after {self.max_retries} attempts")
                    
            except Exception as e:
                logger.error(f"Unexpected error scraping {url}: {e}")
                break
                
        return None

    def _extract_timestamp(self, soup) -> str:
        """Extract timestamp from article soup."""
        timestamp_selectors = [
            'div.ok-news-post-hour span',
            '.timestamp',
            '.post-date',
            '[class*="date"]'
        ]
        
        for selector in timestamp_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        
        return 'No timestamp found'

    def _extract_category(self, soup, url: str) -> str:
        """Extract article category from soup or URL."""
        category_selectors = [
            '.category-name',
            '.post-category',
            '[class*="category"]',
        ]
        
        for selector in category_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        
        # Fallback: extract from URL
        url_parts = url.split('/')
        categories = ['politics', 'sports', 'economy', 'technology', 'entertainment', 'health']
        for part in url_parts:
            if part.lower() in categories:
                return part
        
        return 'general'

    def _extract_author(self, soup) -> str:
        """Extract author information from soup."""
        author_selectors = [
            '.author-name',
            '.post-author',
            '[class*="author"]',
            '.byline'
        ]
        
        for selector in author_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text(strip=True)
        
        return 'Unknown'

    def get_article_links_for_month(self, year: int, month: int, max_articles: int = 100) -> List[str]:
        """
        Get article links for a specific month with pagination handling.
        
        Args:
            year: Target year
            month: Target month
            max_articles: Maximum articles to collect
            
        Returns:
            List of article URLs
        """
        base_url = "https://www.onlinekhabar.com/content/news/page/"
        links = set()
        page = 1
        month_str = str(month).zfill(2)
        
        logger.info(f"Collecting articles from {year}/{month_str}")
        
        while len(links) < max_articles and page <= 20:
            url = f"{base_url}{page}"
            logger.debug(f"Processing page {page}")
            
            try:
                response = requests.get(url, headers=self.headers, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                article_links = soup.find_all('a', href=True)
                
                month_links = 0
                for link in article_links:
                    href = link['href']
                    if f"https://www.onlinekhabar.com/{year}/{month_str}/" in href:
                        links.add(href)
                        month_links += 1
                
                logger.debug(f"Found {month_links} articles on page {page}")
                
                # Check if we've reached the last page
                next_page = soup.find('a', class_='next page-numbers')
                if not next_page:
                    logger.info(f"Reached last page for {year}/{month_str}")
                    break
                
                page += 1
                time.sleep(self.delay_between_requests + random.uniform(0, 1))
                
            except Exception as e:
                logger.error(f"Error processing page {page}: {e}")
                break
        
        logger.info(f"Collected {len(links)} article links for {year}/{month_str}")
        return list(links)

    def scrape_articles_bulk(self, urls: List[str]) -> List[Dict]:
        """
        Scrape multiple articles with progress tracking.
        
        Args:
            urls: List of URLs to scrape
            
        Returns:
            List of successfully scraped articles
        """
        articles = []
        
        for url in tqdm(urls, desc="Scraping articles"):
            article = self.scrape_article(url)
            if article:
                articles.append(article)
                self.scraped_articles.append(article)
            
            # Rate limiting
            sleep_time = self.delay_between_articles + random.uniform(0, 0.5)
            time.sleep(sleep_time)
        
        return articles

    def scrape_last_n_months(self, n_months: int = 6, articles_per_month: int = 100) -> List[Dict]:
        """
        Scrape articles from the last n months.
        
        Args:
            n_months: Number of months to go back
            articles_per_month: Maximum articles per month
            
        Returns:
            List of all scraped articles
        """
        all_articles = []
        current_date = datetime.now()
        
        logger.info(f"Starting scraping of last {n_months} months")
        
        for i in range(n_months):
            target_date = current_date - timedelta(days=30*i)
            year = target_date.year
            month = target_date.month
            
            # Get article URLs for this month
            article_urls = self.get_article_links_for_month(year, month, articles_per_month)
            
            if article_urls:
                # Scrape articles for this month
                month_articles = self.scrape_articles_bulk(article_urls)
                all_articles.extend(month_articles)
                
                logger.info(f"Scraped {len(month_articles)} articles from {year}/{month}")
                
                # Break between months
                if i < n_months - 1:
                    break_time = 5 + random.uniform(0, 3)
                    logger.info(f"Taking {break_time:.1f}s break before next month")
                    time.sleep(break_time)
            else:
                logger.warning(f"No articles found for {year}/{month}")
        
        logger.info(f"Scraping completed. Total articles: {len(all_articles)}")
        return all_articles

    def save_articles(self, filename: str, articles: List[Dict] = None):
        """
        Save scraped articles to JSON file.
        
        Args:
            filename: Output filename
            articles: Articles to save (uses self.scraped_articles if None)
        """
        if articles is None:
            articles = self.scraped_articles
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(articles, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved {len(articles)} articles to {filename}")
            
            # Log basic statistics
            if articles:
                avg_length = sum(len(a['content']) for a in articles) / len(articles)
                logger.info(f"Average article length: {avg_length:.0f} characters")
                
                categories = {}
                for article in articles:
                    cat = article.get('category', 'unknown')
                    categories[cat] = categories.get(cat, 0) + 1
                
                logger.info(f"Category distribution: {categories}")
                
        except Exception as e:
            logger.error(f"Error saving articles to {filename}: {e}")

    def get_scraping_stats(self) -> Dict:
        """Get comprehensive scraping statistics."""
        if not self.scraped_articles:
            return {"total_articles": 0}
        
        stats = {
            "total_articles": len(self.scraped_articles),
            "average_content_length": sum(len(a['content']) for a in self.scraped_articles) / len(self.scraped_articles),
            "date_range": {
                "earliest": min(a.get('scraped_at', '') for a in self.scraped_articles),
                "latest": max(a.get('scraped_at', '') for a in self.scraped_articles)
            },
            "categories": {}
        }
        
        # Category distribution
        for article in self.scraped_articles:
            cat = article.get('category', 'unknown')
            stats["categories"][cat] = stats["categories"].get(cat, 0) + 1
        
        return stats


def main():
    """Main execution function for standalone scraping."""
    logger.info("Starting Enhanced Nepali News Scraper")
    
    # Initialize scraper
    scraper = EnhancedNepaliNewsScraper(delay_between_requests=2.0, max_retries=3)
    
    # Configuration
    n_months = 6
    articles_per_month = 100
    output_filename = "onlinekhabar_scraped_articles.json"
    
    try:
        # Scrape articles
        articles = scraper.scrape_last_n_months(n_months, articles_per_month)
        
        # Save results
        scraper.save_articles(output_filename)
        
        # Display statistics
        stats = scraper.get_scraping_stats()
        print("\n" + "="*50)
        print("SCRAPING COMPLETED SUCCESSFULLY")
        print("="*50)
        print(f"Total articles scraped: {stats['total_articles']}")
        print(f"Average content length: {stats['average_content_length']:.0f} characters")
        print(f"Category distribution: {stats['categories']}")
        print(f"Data saved to: {output_filename}")
        print("\nReady for analytics processing!")
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}")
        raise


if __name__ == "__main__":
    main()
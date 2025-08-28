import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import json
import time
from datetime import datetime, timedelta
import calendar
import random

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

# Configure delays between requests
DELAY_BETWEEN_REQUESTS = 2  # seconds between page requests
DELAY_BETWEEN_ARTICLES = 1  # seconds between article scraping
MAX_RETRIES = 3  # maximum number of retries for failed requests

def scrape_article(url):
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, headers=headers, timeout=15)
            r.raise_for_status()  # Raise an exception for bad status codes
            
            soup = BeautifulSoup(r.content, 'html.parser')

            # Find the title (use a fallback if missing)
            title_tag = soup.find('h1')
            content_div = soup.find('div', class_='ok18-single-post-content-wrap')

            # timestamp
            timestamp_div = soup.find('div', class_='ok-news-post-hour')
            timestamp_span = timestamp_div.find('span') if timestamp_div else None
            timestamp = timestamp_span.get_text(strip=True) if timestamp_span else 'No timestamp found'
            
            if not title_tag or not content_div:
                print(f"⚠️ Skipping (structure missing): {url}")
                return None

            title = title_tag.get_text(strip=True)
            paragraphs = content_div.find_all('p')
            content = ' '.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))

            if len(content) < 100:
                print(f"⚠️ Skipping (too short): {url}")
                return None

            return {
                "url": url,
                "timestamp": timestamp,
                "title": title,
                "content": content
            }

        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed on {url} (attempt {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                # Exponential backoff with jitter
                sleep_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"⏳ Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                print(f"💥 Failed after {MAX_RETRIES} attempts: {url}")
                return None
        except Exception as e:
            print(f"❌ Unexpected error on {url}: {e}")
            return None

def get_article_links_for_month(year, month):
    """Get article links for a specific month by iterating through pages"""
    base_url = f"https://www.onlinekhabar.com/content/news/page/"
    links = set()
    page = 1
    
    # Format month with leading zero if needed
    month_str = str(month).zfill(2)
    
    print(f"📅 Scraping articles from {year}/{month_str}")
    
    # Continue until we have at least 100 links for the month or reach page limit
    while len(links) < 100 and page <= 20:  # Limit to 20 pages per month
        url = f"{base_url}{page}"
        print(f"📄 Processing page {page}")
        
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(url, headers=headers, timeout=15)
                r.raise_for_status()
                
                soup = BeautifulSoup(r.content, 'html.parser')
                
                # Find all article links on the page
                article_links = soup.find_all('a', href=True)
                
                # Filter links for the target month
                month_links = 0
                for a in article_links:
                    href = a['href']
                    if f"https://www.onlinekhabar.com/{year}/{month_str}/" in href:
                        links.add(href)
                        month_links += 1
                
                print(f"✅ Found {month_links} articles on page {page}")
                
                # Check if we've reached the last page
                next_page = soup.find('a', class_='next page-numbers')
                if not next_page:
                    print(f"⏹️ No more pages for {year}/{month_str}")
                    break
                    
                break  # Break out of retry loop if successful
                
            except requests.exceptions.RequestException as e:
                print(f"❌ Error processing page {page} (attempt {attempt+1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    sleep_time = (2 ** attempt) + random.uniform(0, 1)
                    print(f"⏳ Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    print(f"💥 Failed to process page {page} after {MAX_RETRIES} attempts")
                    break
            except Exception as e:
                print(f"❌ Unexpected error processing page {page}: {e}")
                break
        
        page += 1
        # Add delay with some randomness to avoid patterns
        sleep_time = DELAY_BETWEEN_REQUESTS + random.uniform(0, 1)
        time.sleep(sleep_time)
    
    return list(links)

# Entry point
scraped_articles = []

# Calculate the last 6 months
current_date = datetime.now()
for i in range(6):
    # Go back i months from current date
    target_date = current_date - timedelta(days=30*i)
    year = target_date.year
    month = target_date.month
    
    # Get article links for this month
    article_urls = get_article_links_for_month(year, month)
    
    print(f"🔗 Found {len(article_urls)} article links for {year}/{month}.")
    
    # Scrape each article
    for url in tqdm(article_urls[:100], desc=f"Scraping {year}/{month}"):  # Limit to 100 per month
        article = scrape_article(url)
        if article:
            scraped_articles.append(article)
        
        # Add delay with some randomness
        sleep_time = DELAY_BETWEEN_ARTICLES + random.uniform(0, 0.5)
        time.sleep(sleep_time)
    
    # Add a longer break between months
    if i < 5:  # Don't wait after the last month
        monthly_break = 5 + random.uniform(0, 3)
        print(f"⏳ Taking a {monthly_break:.1f} second break before next month...")
        time.sleep(monthly_break)

# Save the results
with open("onlinekhabar_scraped_articles.json", "w", encoding="utf-8") as f:
    json.dump(scraped_articles, f, ensure_ascii=False, indent=2)

print(f"✅ Scraped and saved {len(scraped_articles)} articles from the last 6 months.")

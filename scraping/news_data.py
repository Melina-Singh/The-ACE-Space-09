import requests
import pandas as pd
import json
import time
from datetime import datetime, timedelta
import os
from typing import List, Dict
import logging

class ExtendedAECNewsCollector:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = 'https://newsapi.org/v2/everything'
        self.collected_data = []
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging to track collection process"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('aec_collection.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_extended_date_ranges(self, months_back: int = 1) -> List[tuple]:
        """Generate date ranges for 1 month of data collection"""
        ranges = []
        end_date = datetime.now()
        total_days = months_back * 30  # 30 days for 1 month
        for i in range(0, total_days, 3):
            start = end_date - timedelta(days=i+3)
            end = end_date - timedelta(days=i)
            ranges.append((start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))
        self.logger.info(f"Created {len(ranges)} date ranges covering {total_days} days")
        return ranges

    def comprehensive_search_queries(self) -> List[Dict[str, str]]:
        """Expanded queries with categories for AEC topics"""
        return [
            {'query': 'AEC industry OR Architecture Engineering Construction', 'category': 'industry_general'},
            {'query': 'construction industry OR building industry OR construction sector', 'category': 'industry_general'},
            {'query': 'BIM OR Building Information Modeling OR Autodesk OR Revit OR Bentley', 'category': 'technology'},
            {'query': 'construction technology OR digital construction OR smart construction', 'category': 'technology'},
            {'query': 'construction automation OR robotics OR drones OR artificial intelligence OR AI', 'category': 'technology'},
            {'query': 'virtual reality OR VR OR augmented reality OR AR OR mixed reality', 'category': 'technology'},
            {'query': 'sustainable construction OR green building OR LEED OR net zero', 'category': 'sustainability'},
            {'query': 'carbon neutral OR renewable energy OR energy efficient OR sustainable materials', 'category': 'sustainability'},
            {'query': 'infrastructure development OR civil engineering OR heavy construction', 'category': 'infrastructure'},
            {'query': 'modular construction OR prefab OR offsite construction OR mass timber', 'category': 'methods'},
            {'query': 'residential construction OR commercial construction OR mixed-use', 'category': 'project_types'},
            {'query': 'construction costs OR material prices OR labor shortage OR supply chain', 'category': 'economics'},
            {'query': 'construction contracts OR project management OR scheduling OR cost estimation', 'category': 'business'},
            {'query': 'construction safety OR workplace OR OSHA OR regulations OR compliance', 'category': 'safety'},
            {'query': 'construction materials OR concrete OR steel OR timber OR heavy equipment', 'category': 'materials'}
        ]

    def fetch_articles_with_retry(self, query_info: Dict, from_date: str, to_date: str, max_retries: int = 3) -> Dict:
        """Fetch articles with retry logic"""
        params = {
            'q': query_info['query'],
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': 100,
            'apiKey': self.api_key,
            'from': from_date,
            'to': to_date
        }
        for attempt in range(max_retries):
            try:
                response = requests.get(self.base_url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                article_count = len(data.get('articles', []))
                self.logger.info(f"Fetched {article_count} articles for {query_info['category']} ({from_date} to {to_date})")
                if article_count == 0:
                    self.logger.warning(f"No articles found for {query_info['category']} in {from_date} to {to_date}")
                return data
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {query_info['category']}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
        self.logger.error(f"All attempts failed for {query_info['category']} ({from_date} to {to_date})")
        return {}

    def collect_extended_data(self, months_back: int = 1, daily_request_limit: int = 100):
        """Collect 1 month of data with rate limiting"""
        date_ranges = self.get_extended_date_ranges(months_back)
        queries = self.comprehensive_search_queries()
        total_requests_needed = len(queries) * len(date_ranges)
        days_needed = (total_requests_needed / daily_request_limit) + 1
        
        self.logger.info(f"Collection Plan: {len(date_ranges)} date ranges, {len(queries)} queries, "
                        f"{total_requests_needed} total requests, ~{days_needed:.1f} days needed")
        
        requests_today = 0
        collection_start = datetime.now()
        
        for query_info in queries:
            self.logger.info(f"Collecting for category: {query_info['category']}")
            for from_date, to_date in date_ranges:
                if requests_today >= daily_request_limit:
                    self.logger.info("Daily API limit reached. Resuming tomorrow...")
                    self.save_checkpoint()
                    return self.collected_data
                
                data = self.fetch_articles_with_retry(query_info, from_date, to_date)
                requests_today += 1
                
                if data and 'articles' in data:
                    for article in data['articles']:
                        article_data = {
                            'title': article.get('title', ''),
                            'description': article.get('description', ''),
                            'content': article.get('content', ''),
                            'url': article.get('url', ''),
                            'source': article.get('source', {}).get('name', ''),
                            'published_at': article.get('publishedAt', ''),
                            'author': article.get('author', ''),
                            'url_to_image': article.get('urlToImage', ''),
                            'query_category': query_info['category'],
                            'query_used': query_info['query'],
                            'collection_date': datetime.now().isoformat(),
                            'date_range': f"{from_date}_to_{to_date}"
                        }
                        if not any(existing['url'] == article_data['url'] for existing in self.collected_data):
                            self.collected_data.append(article_data)
                
                time.sleep(1)
        
        self.save_checkpoint()
        collection_end = datetime.now()
        self.logger.info(f"Collection completed in {collection_end - collection_start}")
        self.logger.info(f"Total unique articles: {len(self.collected_data)}")
        return self.collected_data

    def save_checkpoint(self):
        """Save collected data to JSON"""
        checkpoint_filename = f"aec_checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(checkpoint_filename, 'w', encoding='utf-8') as f:
            json.dump(self.collected_data, f, indent=2, ensure_ascii=False)
        self.logger.info(f"Checkpoint saved: {checkpoint_filename} ({len(self.collected_data)} articles)")

    def analyze_extended_dataset(self):
        """Analyze collected dataset"""
        if not self.collected_data:
            print("No data to analyze.")
            return
        
        df = pd.DataFrame(self.collected_data)
        print(f"\n{'='*60}\nEXTENDED AEC DATASET ANALYSIS\n{'='*60}")
        print(f"\nDataset Overview:\n  Total articles: {len(df):,}\n  Unique sources: {df['source'].nunique()}\n"
              f"  Date range: {df['published_at'].min()} to {df['published_at'].max()}")
        print(f"\nCategory Distribution:")
        for category, count in df['query_category'].value_counts().head(10).items():
            print(f"  {category}: {count:,} articles")
        print(f"\nTop News Sources:")
        for source, count in df['source'].value_counts().head(10).items():
            print(f"  {source}: {count:,} articles")

def collect_comprehensive_aec_data():
    """Collect 1 month of AEC data"""
    api_key = "22948f2875b84a6bafc6ec72dacebb87"
    collector = ExtendedAECNewsCollector(api_key)
    print("Starting AEC data collection for 1 month...")
    print("Estimated time: ~2 days (100 requests/day limit)")
    
    data = collector.collect_extended_data(months_back=1, daily_request_limit=100)
    collector.analyze_extended_dataset()
    
    final_filename = f"aec_comprehensive_dataset_{datetime.now().strftime('%Y%m%d')}"
    with open(f"{final_filename}.json", 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    df = pd.DataFrame(data)
    df.to_csv(f"{final_filename}.csv", index=False, encoding='utf-8')
    
    print(f"\nFinal dataset saved as:\n  - {final_filename}.json\n  - {final_filename}.csv")
    return df

if __name__ == "__main__":
    collect_comprehensive_aec_data()
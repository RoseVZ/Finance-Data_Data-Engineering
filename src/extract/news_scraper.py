
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

class NewsScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def scrape_yahoo_finance(self, symbol):
        """Scrape news from Yahoo Finance - Updated for new structure"""
        # Use the main quote page which has news
        url = f'https://finance.yahoo.com/quote/{symbol}'
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            articles = []
            
            # Method 1: Try to find news section
            news_items = soup.find_all('h3', class_='Mb(5px)')
            
            if not news_items:
                # Method 2: Try alternative structure
                news_items = soup.find_all('a', class_='subtle-link')
            
            if not news_items:
                # Method 3: Find any links with news-like content
                news_section = soup.find('section', {'data-test': 'news-stream'})
                if news_section:
                    news_items = news_section.find_all('a')
            
            for item in news_items[:10]:  # Get top 10 articles
                try:
                    # Extract title
                    title = item.get_text().strip()
                    
                    # Skip if too short (likely not a real article)
                    if len(title) < 20:
                        continue
                    
                    # Extract link
                    link = item.get('href', '')
                    
                    # Make full URL
                    if link and not link.startswith('http'):
                        link = f'https://finance.yahoo.com{link}'
                    
                    if title and link:
                        articles.append({
                            'symbol': symbol,
                            'title': title,
                            'url': link,
                            'source': 'Yahoo Finance',
                            'scraped_at': datetime.now()
                        })
                except Exception as e:
                    continue
            
            # If still no articles, use alternative news source
            if len(articles) == 0:
                print(f"⚠️  Yahoo Finance scraping failed, trying alternative source...")
                articles = self._scrape_alternative_source(symbol)
            
            df = pd.DataFrame(articles)
            print(f"✅ Scraped {len(df)} news articles for {symbol}")
            return df
            
        except Exception as e:
            print(f"❌ Error scraping news for {symbol}: {str(e)}")
            # Try alternative source as fallback
            return self._scrape_alternative_source(symbol)
    
    def _scrape_alternative_source(self, symbol):
        """Scrape from MarketWatch as alternative"""
        url = f'https://www.marketwatch.com/investing/stock/{symbol.lower()}'
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            articles = []
            
            # Find news headlines
            news_items = soup.find_all('a', class_='link')
            
            for item in news_items[:10]:
                try:
                    title = item.get_text().strip()
                    
                    if len(title) < 20:
                        continue
                    
                    link = item.get('href', '')
                    if link and not link.startswith('http'):
                        link = f'https://www.marketwatch.com{link}'
                    
                    if title and link:
                        articles.append({
                            'symbol': symbol,
                            'title': title,
                            'url': link,
                            'source': 'MarketWatch',
                            'scraped_at': datetime.now()
                        })
                except Exception:
                    continue
            
            df = pd.DataFrame(articles)
            print(f"✅ Scraped {len(df)} news articles from MarketWatch for {symbol}")
            return df
            
        except Exception as e:
            print(f"❌ Alternative source also failed: {str(e)}")
            return pd.DataFrame()
    
    def scrape_finviz_news(self, symbol):
        """Scrape from Finviz (another alternative)"""
        url = f'https://finviz.com/quote.ashx?t={symbol}'
        
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            articles = []
            news_table = soup.find('table', class_='fullview-news-outer')
            
            if news_table:
                rows = news_table.find_all('tr')
                
                for row in rows[:10]:
                    try:
                        link_tag = row.find('a')
                        if link_tag:
                            title = link_tag.get_text().strip()
                            link = link_tag.get('href', '')
                            
                            if title and link:
                                articles.append({
                                    'symbol': symbol,
                                    'title': title,
                                    'url': link,
                                    'source': 'Finviz',
                                    'scraped_at': datetime.now()
                                })
                    except Exception:
                        continue
            
            df = pd.DataFrame(articles)
            print(f"✅ Scraped {len(df)} news articles from Finviz for {symbol}")
            return df
            
        except Exception as e:
            print(f"❌ Finviz scraping failed: {str(e)}")
            return pd.DataFrame()
    
    def scrape_multiple_symbols(self, symbols):
        """Scrape news for multiple symbols"""
        all_news = []
        
        for symbol in symbols:
            # Try Yahoo first
            df = self.scrape_yahoo_finance(symbol)
            
            # If Yahoo fails, try Finviz
            if df.empty:
                df = self.scrape_finviz_news(symbol)
            
            if not df.empty:
                all_news.append(df)
            
            time.sleep(2)  # Be respectful to servers
        
        if all_news:
            return pd.concat(all_news, ignore_index=True)
        return pd.DataFrame()

# Test
if __name__ == "__main__":
    scraper = NewsScraper()
    
    # Test Yahoo Finance
    print("Testing Yahoo Finance...")
    df = scraper.scrape_yahoo_finance('AAPL')
    print(df.head() if not df.empty else "No results from Yahoo")
    
    # Test Finviz
    print("\nTesting Finviz...")
    df = scraper.scrape_finviz_news('AAPL')
    print(df.head() if not df.empty else "No results from Finviz")
import time
from datetime import datetime
import json
import pandas as pd
import logging
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re

# Setup logging
logging.basicConfig()
    level=logging.INFO
import time
from datetime import datetime
import json
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import re
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tender_scrape.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def scrape_tender_impulse(max_tenders: int = 10) -> list:
    """Scrape tenders from Tender Impulse"""
    tenders = []
    url = 'https://tenderimpulse.com/'
    logger.info(f"Scraping tenders from {url} (max {max_tenders})")
    
    # Setup Selenium
    options = Options()
    options.add_argument('--headless')  # Run without browser UI
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options=options)
    
    try:
        driver.get(url)
        time.sleep(10)  # Extended wait for dynamic content
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Updated selectors for tender listings
        tender_items = soup.find_all('div', class_=re.compile('listing-item|card|tender|post', re.I))[:max_tenders]
        
        for item in tender_items:
            title_tag = item.find('h4') or item.find('h3') or item.find('h2') or item.find('a')
            link_tag = item.find('a', href=True)
            date_tag = item.find('time') or item.find('span', class_=re.compile('date|time|posted', re.I))
            
            if title_tag and link_tag:
                tender_url = link_tag['href']
                if not tender_url.startswith('http'):
                    tender_url = url.rstrip('/') + tender_url
                
                tender_data = {
                    'title': title_tag.get_text(strip=True),
                    'description': '',
                    'content': '',
                    'url': tender_url,
                    'source': 'Tender Impulse',
                    'published_at': date_tag.get_text(strip=True) if date_tag else '',
                    'author': '',
                    'url_to_image': '',
                    'query_category': 'tenders',
                    'query_used': 'tender_scrape',
                    'collection_date': datetime.now().isoformat(),
                    'platform': 'Tender Impulse'
                }
                if not any(existing['url'] == tender_data['url'] for existing in tenders):
                    tenders.append(tender_data)
        
        logger.info(f"Scraped {len(tenders)} tenders")
    except Exception as e:
        logger.error(f"Failed to scrape: {e}")
    finally:
        driver.quit()
    
    logger.info(f"Total unique tenders: {len(tenders)}")
    return tenders

def generate_pdf(tenders: list, filename: str):
    """Generate a PDF using ReportLab"""
    try:
        pdf = SimpleDocTemplate(f"{filename}.pdf", pagesize=A4)
        elements = []
        
        # Styles
        styles = getSampleStyleSheet()
        title = Paragraph("Tender Impulse Dataset", styles['Title'])
        elements.append(title)
        
        # Table data
        data = [['Title', 'URL', 'Source', 'Published At', 'Collection Date']]
        for tender in tenders:
            title = tender['title'][:50] + '...' if len(tender['title']) > 50 else tender['title']
            url = tender['url'][:30] + '...' if len(tender['url']) > 30 else tender['url']
            source = tender['source']
            published_at = tender['published_at']
            collection_date = tender['collection_date'][:19].replace('T', ' ')
            data.append([title, url, source, published_at, collection_date])
        
        # Create table
        table = Table(data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        elements.append(table)
        
        # Build PDF
        pdf.build(elements)
        logger.info(f"PDF generated: {filename}.pdf")
    except Exception as e:
        logger.error(f"Failed to generate PDF: {e}")

def main():
    """Scrape tenders and save as JSON, CSV, and PDF"""
    print("Scraping tenders from Tender Impulse...")
    tenders = scrape_tender_impulse(max_tenders=10)
    
    # Save to JSON
    final_filename = f"tender_dataset_{datetime.now().strftime('%Y%m%d')}"
    with open(f"{final_filename}.json", 'w', encoding='utf-8') as f:
        json.dump(tenders, f, indent=2, ensure_ascii=False)
    
    # Save to CSV
    df = pd.DataFrame(tenders)
    df.to_csv(f"{final_filename}.csv", index=False, encoding='utf-8')
    
    # Save to PDF
    generate_pdf(tenders, final_filename)
    
    print(f"\nDataset saved
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tender_scrape.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def scrape_tender_impulse(max_tenders: int = 10) -> list:
    """Scrape tenders from Tender Impulse"""
    tenders = []
    url = 'https://tenderimpulse.com/'
    logger.info(f"Scraping tenders from {url} (max {max_tenders})")
    
    # Setup Selenium
    options = Options()
    options.add_argument('--headless')  # Run without browser UI
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options=options)
    
    try:
        driver.get(url)
        time.sleep(5)  # Wait for dynamic content
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find tender listings (adjust selectors based on site structure)
        tender_items = soup.find_all('div', class_=re.compile('tender|item|listing|post', re.I))[:max_tenders]
        
        for item in tender_items:
            title_tag = item.find('h2') or item.find('h3') or item.find('a')
            link_tag = item.find('a', href=True)
            date_tag = item.find('time') or item.find('span', class_=re.compile('date|time', re.I))
            
            if title_tag and link_tag:
                tender_url = link_tag['href']
                if not tender_url.startswith('http'):
                    tender_url = url.rstrip('/') + tender_url
                
                tender_data = {
                    'title': title_tag.get_text(strip=True),
                    'description': '',
                    'content': '',
                    'url': tender_url,
                    'source': 'Tender Impulse',
                    'published_at': date_tag.get_text(strip=True) if date_tag else '',
                    'author': '',
                    'url_to_image': '',
                    'query_category': 'tenders',
                    'query_used': 'tender_scrape',
                    'collection_date': datetime.now().isoformat(),
                    'platform': 'Tender Impulse'
                }
                if not any(existing['url'] == tender_data['url'] for existing in tenders):
                    tenders.append(tender_data)
        
        logger.info(f"Scraped {len(tenders)} tenders")
    except Exception as e:
        logger.error(f"Failed to scrape: {e}")
    finally:
        driver.quit()
    
    logger.info(f"Total unique tenders: {len(tenders)}")
    return tenders

def generate_latex_pdf(tenders: list, filename: str):
    """Generate a PDF using LaTeX"""
    # Creating LaTeX document
    latex_content = r"""
\documentclass[a4paper,12pt]{article}
\usepackage[utf8]{inputenc}
\usepackage[T1]{fontenc}
\usepackage{booktabs}
\usepackage{longtable}
\usepackage{hyperref}
\usepackage{geometry}
\geometry{margin=1in}
\usepackage{noto}

\title{Tender Impulse Dataset}
\author{}
\date{\today}

\begin{document}
\maketitle

\section*{Tender Listings}
\begin{longtable}{p{5cm}p{6cm}p{2cm}p{3cm}p{3cm}}
\toprule
\textbf{Title} & \textbf{URL} & \textbf{Source} & \textbf{Published At} & \textbf{Collection Date} \\
\midrule
\endhead
"""

    # Adding tender data
    for tender in tenders:
        # Escaping special LaTeX characters
        title = tender['title'].replace('&', r'\&').replace('_', r'\_').replace('#', r'\#')
        url = tender['url'].replace('_', r'\_')
        source = tender['source'].replace('&', r'\&')
        published_at = tender['published_at'].replace('&', r'\&')
        collection_date = tender['collection_date'][:19].replace('T', ' ')  # Format ISO date
        
        latex_content += f"{title} & \\href{{{url}}}{{Link}} & {source} & {published_at} & {collection_date} \\\\\n"
    
    # Closing LaTeX document
    latex_content += r"""
\bottomrule
\end{longtable}
\end{document}
"""

    # Writing LaTeX file
    tex_filename = f"{filename}.tex"
    with open(tex_filename, 'w', encoding='utf-8') as f:
        f.write(latex_content)
    
    # Compiling LaTeX to PDF
    try:
        import subprocess
        subprocess.run(['latexmk', '-pdf', '-interaction=nonstopmode', tex_filename], check=True)
        logger.info(f"PDF generated: {filename}.pdf")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to generate PDF: {e}")
    except FileNotFoundError:
        logger.error("latexmk not found. Ensure TeX Live is installed.")

def main():
    """Scrape tenders and save as JSON, CSV, and PDF"""
    print("Scraping tenders from Tender Impulse...")
    tenders = scrape_tender_impulse(max_tenders=10)
    
    # Save to JSON
    final_filename = f"tender_dataset_{datetime.now().strftime('%Y%m%d')}"
    with open(f"{final_filename}.json", 'w', encoding='utf-8') as f:
        json.dump(tenders, f, indent=2, ensure_ascii=False)
    
    # Save to CSV
    df = pd.DataFrame(tenders)
    df.to_csv(f"{final_filename}.csv", index=False, encoding='utf-8')
    
    # Save to PDF
    generate_latex_pdf(tenders, final_filename)
    
    print(f"\nDataset saved as:\n  - {final_filename}.json\n  - {final_filename}.csv\n  - {final_filename}.pdf")
    print(f"Total tenders: {len(tenders)}")

if __name__ == "__main__":
    main()
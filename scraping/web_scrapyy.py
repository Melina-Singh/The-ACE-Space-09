import os
import requests
import csv
import json
from bs4 import BeautifulSoup
import time

# Settings
base_url = "https://aecmag.com/news/page/{}/"
folder_name = "aec_news_data"
csv_file_name = "aec_articles.csv"
json_file_name = "aec_articles.json"
max_pages = 50  # First 20 pages

# Create output folder
os.makedirs(folder_name, exist_ok=True)

article_data = []

# Loop through first 20 pages
for page_num in range(1, max_pages + 1):
    url = base_url.format(page_num)
    print(f"🔍 Scraping page {page_num}: {url}")
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Failed to load page {page_num}. Status code: {response.status_code}")
        continue

    soup = BeautifulSoup(response.text, "html.parser")
    articles = soup.find_all("article")

    if not articles:
        print(f"⚠️ No articles found on page {page_num}.")
        break

    for article in articles:
        title_tag = article.find("h2")
        link_tag = article.find("a", href=True)
        summary_tag = article.find("p")

        title = title_tag.text.strip() if title_tag else "No title"
        link = link_tag['href'] if link_tag else "No link"
        summary = summary_tag.text.strip() if summary_tag else "No summary"

        article_data.append({
            "title": title,
            "link": link,
            "summary": summary
        })

    time.sleep(1)  # Be respectful to server

# Save to CSV
csv_path = os.path.join(folder_name, csv_file_name)
with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["Title", "Link", "Summary"])
    for article in article_data:
        writer.writerow([article["title"], article["link"], article["summary"]])

# Save to JSON
json_path = os.path.join(folder_name, json_file_name)
with open(json_path, 'w', encoding='utf-8') as json_file:
    json.dump(article_data, json_file, ensure_ascii=False, indent=4)

print(f"\n✅ {len(article_data)} articles scraped and saved from 20 pages.")
print(f"📄 CSV saved at:  {csv_path}")
print(f"📄 JSON saved at: {json_path}")

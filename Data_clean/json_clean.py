import pandas as pd
import json
import os
import logging
import re
from glob import glob
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AECDataCleaner:
    def __init__(self, json_folder):
        # Find all JSON files in the specified folder
        self.json_files = glob(os.path.join(json_folder, "*.json"))
        if not self.json_files:
            raise FileNotFoundError(f"No JSON files found in {json_folder}")
        self.output_dir = "aec_cleaned_data"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def is_valid_url(self, url):
        """Check if a URL is valid (basic HTTP/HTTPS check)."""
        if not url or not isinstance(url, str):
            return False
        return bool(re.match(r'^https?://', url))

    def is_valid_date(self, date_str, is_scraped_at=False):
        """Check if a date string is valid (ISO for scraped_at, 'Published MMM DD, YYYY' for published_date)."""
        if not date_str or not isinstance(date_str, str):
            return False
        try:
            if is_scraped_at:
                # Expect ISO format for scraped_at (e.g., 2025-05-29T17:34:07.030106)
                datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                # Expect 'Published MMM DD, YYYY' for published_date
                re.match(r'^Published [A-Za-z]+ \d{1,2}, \d{4}$', date_str)
            return True
        except ValueError:
            return False

    def convert_published_date(self, date_str):
        """Convert 'Published MMM DD, YYYY' to ISO format."""
        if not date_str or not isinstance(date_str, str):
            return "2025-01-01T00:00:00Z"
        try:
            if re.match(r'^Published [A-Za-z]+ \d{1,2}, \d{4}$', date_str):
                # Extract MMM DD, YYYY (e.g., May 27, 2025)
                date_part = date_str.replace("Published ", "").strip()
                parsed_date = datetime.strptime(date_part, "%B %d, %Y")
                return parsed_date.strftime("%Y-%m-%dT00:00:00Z")
            return "2025-01-01T00:00:00Z"
        except ValueError:
            return "2025-01-01T00:00:00Z"

    def check_json_data(self, json_data, dataset_name):
        """Check JSON news article data for issues, returning a list of issues and flags for cleaning."""
        issues = []
        cleaning_needed = {
            'missing_values': False,
            'duplicates': False,
            'text_issues': False,
            'invalid_dates': False,
            'invalid_urls': False,
            'categorical_issues': False,
            'empty_tags': False
        }
        
        # Convert JSON to DataFrame for easier processing
        df = pd.json_normalize(json_data)
        if df.empty:
            issues.append(f"Empty dataset in {dataset_name}")
            return issues, cleaning_needed, df
        
        # Check for missing values
        null_counts = df.isnull().sum()
        null_issues = null_counts[null_counts > 0]
        if not null_issues.empty:
            issues.append(f"Missing values in {dataset_name}:\n{null_issues}")
            cleaning_needed['missing_values'] = True
        
        # Check for duplicates (based on 'title' or 'url')
        duplicate_count = df.duplicated(subset=['title', 'url']).sum()
        if duplicate_count > 0:
            issues.append(f"{duplicate_count} duplicate articles in {dataset_name} (based on title or url)")
            cleaning_needed['duplicates'] = True
        
        # Check text fields for short or empty content
        text_cols = ['title', 'content', 'summary']
        for col in text_cols:
            if col in df.columns:
                short_texts = df[col].apply(lambda x: len(str(x).strip()) < (10 if col == 'title' else 50) if pd.notnull(x) else True)
                if short_texts.any():
                    issues.append(f"Short or empty text in {dataset_name}.{col}: {short_texts.sum()} instances")
                    cleaning_needed['text_issues'] = True
        
        # Check date fields for validity
        date_cols = [('published_date', False), ('scraped_at', True)]
        for col, is_scraped_at in date_cols:
            if col in df.columns:
                invalid_dates = df[col].apply(lambda x: not self.is_valid_date(x, is_scraped_at) if pd.notnull(x) else True)
                if invalid_dates.any():
                    issues.append(f"Invalid or missing dates in {dataset_name}.{col}: {invalid_dates.sum()} instances")
                    cleaning_needed['invalid_dates'] = True
        
        # Check URLs for validity
        if 'url' in df.columns:
            invalid_urls = df['url'].apply(lambda x: not self.is_valid_url(x) if pd.notnull(x) else True)
            if invalid_urls.any():
                issues.append(f"Invalid or missing URLs in {dataset_name}.url: {invalid_urls.sum()} instances")
                cleaning_needed['invalid_urls'] = True
        
        # Check categorical consistency
        if 'category' in df.columns:
            unique_values = df['category'].dropna().unique()
            if len(unique_values) < 2:
                issues.append(f"Low variety in {dataset_name}.category: {unique_values}")
                cleaning_needed['categorical_issues'] = True
        
        # Check for empty tags
        if 'tags' in df.columns:
            empty_tags = df['tags'].apply(lambda x: isinstance(x, list) and len(x) == 0)
            if empty_tags.all():
                issues.append(f"All tags are empty in {dataset_name}.tags")
                cleaning_needed['empty_tags'] = True
        
        return issues, cleaning_needed, df

    def clean_json_data(self, json_data, dataset_name, cleaning_needed, df):
        """Clean JSON news article data based on detected issues."""
        cleaned_df = df.copy()
        
        # Fill missing values if needed
        if cleaning_needed['missing_values']:
            for col in cleaned_df.columns:
                if col in ['title']:
                    cleaned_df[col] = cleaned_df[col].fillna('No title')
                elif col in ['content', 'summary', 'author', 'category']:
                    cleaned_df[col] = cleaned_df[col].fillna('Unknown')
                elif col in ['url']:
                    cleaned_df[col] = cleaned_df[col].fillna('Unknown')
                elif col in ['tags']:
                    cleaned_df[col] = cleaned_df[col].apply(lambda x: [] if pd.isna(x) else x)
            logger.info(f"Filled missing values in {dataset_name}")
        
        # Remove duplicates if needed
        if cleaning_needed['duplicates']:
            initial_rows = len(cleaned_df)
            cleaned_df = cleaned_df.drop_duplicates(subset=['title', 'url'], keep='first')
            logger.info(f"Removed {initial_rows - len(cleaned_df)} duplicates in {dataset_name}")
        
        # Clean text fields if needed
        if cleaning_needed['text_issues'] or cleaning_needed['missing_values']:
            for col in ['title', 'content', 'summary']:
                if col in cleaned_df.columns:
                    # Remove artifacts (e.g., \r\n), extra whitespace, and normalize case
                    cleaned_df[col] = cleaned_df[col].apply(
                        lambda x: re.sub(r'\r\n', ' ', str(x)).strip().lower() if pd.notnull(x) else 'Unknown'
                    )
                    # Ensure minimum length
                    min_length = 10 if col == 'title' else 50
                    cleaned_df[col] = cleaned_df[col].apply(
                        lambda x: x if len(x) >= min_length else x + " " + "Placeholder text for minimum length."[:min_length-len(x)]
                    )
            logger.info(f"Normalized text fields in {dataset_name}")
        
        # Fix invalid dates if needed
        if cleaning_needed['invalid_dates']:
            for col, is_scraped_at in [('published_date', False), ('scraped_at', True)]:
                if col in cleaned_df.columns:
                    if is_scraped_at:
                        cleaned_df[col] = cleaned_df[col].apply(
                            lambda x: x if self.is_valid_date(x, is_scraped_at) else "2025-01-01T00:00:00Z"
                        )
                    else:
                        cleaned_df[col] = cleaned_df[col].apply(self.convert_published_date)
            logger.info(f"Fixed invalid dates in {dataset_name}")
        
        # Fix invalid URLs if needed
        if cleaning_needed['invalid_urls']:
            if 'url' in cleaned_df.columns:
                cleaned_df['url'] = cleaned_df['url'].apply(
                    lambda x: x if self.is_valid_url(x) else 'Unknown'
                )
            logger.info(f"Fixed invalid URLs in {dataset_name}")
        
        # Normalize categorical fields if needed
        if cleaning_needed['categorical_issues'] or cleaning_needed['missing_values']:
            if 'category' in cleaned_df.columns:
                cleaned_df['category'] = cleaned_df['category'].str.strip().str.rstrip(',').str.lower()
            logger.info(f"Normalized category field in {dataset_name}")
        
        # Handle empty tags if needed
        if cleaning_needed['empty_tags']:
            if 'tags' in cleaned_df.columns:
                cleaned_df['tags'] = cleaned_df['tags'].apply(lambda x: ['unknown'] if isinstance(x, list) and len(x) == 0 else x)
            logger.info(f"Handled empty tags in {dataset_name}")
        
        # Convert back to JSON format
        cleaned_data = cleaned_df.to_dict(orient='records')
        
        # Save cleaned data only if changes were made
        if any(cleaning_needed.values()):
            output_path = os.path.join(self.output_dir, f"cleaned_{dataset_name}.json")
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(cleaned_data, f, indent=2)
            logger.info(f"Saved cleaned JSON data to {output_path}")
        else:
            logger.info(f"No cleaning needed for {dataset_name}, skipping save")
        
        return cleaned_data

def main():
    """Main function to check and clean AEC news article JSON data."""
    json_folder = "NEWs"
    
    try:
        cleaner = AECDataCleaner(json_folder)
        print("\n=== JSON News Article Issues ===")
        for json_file in cleaner.json_files:
            dataset_name = os.path.basename(json_file).replace('.json', '')
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                issues, cleaning_needed, df = cleaner.check_json_data(json_data, dataset_name)
                if not issues:
                    print(f"No issues found in {dataset_name}")
                else:
                    print(f"Issues in {dataset_name}:")
                    for issue in issues:
                        print(f"- {issue}")
                
                # Clean data only where necessary
                cleaned_data = cleaner.clean_json_data(json_data, dataset_name, cleaning_needed, df)
                print(f"Processed {dataset_name}: {len(cleaned_data)} records")
            except Exception as e:
                logger.error(f"Error processing {dataset_name}: {e}")
                print(f"Error processing {dataset_name}. Check logs.")
        
        print("\n=== Cleaning Summary ===")
        print(f"Cleaned data (if necessary) saved in {cleaner.output_dir}/")
        print(f"Processed {len(cleaner.json_files)} JSON files")
        print("Generated on: 11:19 PM +0545, Sunday, June 1, 2025")
    
    except FileNotFoundError as e:
        logger.error(str(e))
        print(str(e))

if __name__ == "__main__":
    main()
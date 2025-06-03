import pandas as pd
import os
import logging
from glob import glob

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AECDataCleaner:
    def __init__(self, csv_folder):
        # Find all CSV files in the specified folder
        self.structured_files = glob(os.path.join(csv_folder, "*.csv"))
        if not self.structured_files:
            raise FileNotFoundError(f"No CSV files found in {csv_folder}")
        self.output_dir = "aec_cleaned_data"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def check_structured_data(self, df, dataset_name):
        """Check structured data for issues, returning a list of issues and flags for cleaning."""
        issues = []
        cleaning_needed = {
            'missing_values': False,
            'duplicates': False,
            'outliers': False,
            'categorical_issues': False
        }
        
        # Check for missing values
        null_counts = df.isnull().sum()
        null_issues = null_counts[null_counts > 0]
        if not null_issues.empty:
            issues.append(f"Missing values in {dataset_name}:\n{null_issues}")
            cleaning_needed['missing_values'] = True
        
        # Check for duplicates
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            issues.append(f"{duplicate_count} duplicate rows in {dataset_name}")
            cleaning_needed['duplicates'] = True
        
        # Check numeric fields for outliers using IQR method
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_cols:
            if col in ['value_usd_m', 'market_size_usd_b', 'investment_potential_usd_m']:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                upper_bound = Q3 + 1.5 * IQR
                outliers = df[df[col] > upper_bound][col]
                if not outliers.empty:
                    issues.append(f"Outliers in {dataset_name}.{col}: {len(outliers)} values > {upper_bound:.2f}")
                    cleaning_needed['outliers'] = True
        
        # Check categorical consistency
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if col in ['sector', 'region', 'country', 'project_scale']:
                unique_values = df[col].unique()
                if len(unique_values) < 2:
                    issues.append(f"Low variety in {dataset_name}.{col}: {unique_values}")
                    cleaning_needed['categorical_issues'] = True
        
        return issues, cleaning_needed

    def clean_structured_data(self, df, dataset_name, cleaning_needed):
        """Clean structured data based on detected issues."""
        cleaned_df = df.copy()
        
        # Fill missing values if needed
        if cleaning_needed['missing_values']:
            for col in cleaned_df.columns:
                if cleaned_df[col].dtype in ['float64', 'int64']:
                    cleaned_df[col] = cleaned_df[col].fillna(cleaned_df[col].median() if cleaned_df[col].notnull().any() else 0)
                elif cleaned_df[col].dtype == 'object':
                    cleaned_df[col] = cleaned_df[col].fillna('Unknown')
            logger.info(f"Filled missing values in {dataset_name}")
        
        # Remove duplicates if needed
        if cleaning_needed['duplicates']:
            initial_rows = len(cleaned_df)
            cleaned_df = cleaned_df.drop_duplicates()
            logger.info(f"Removed {initial_rows - len(cleaned_df)} duplicates in {dataset_name}")
        
        # Cap outliers if needed
        if cleaning_needed['outliers']:
            numeric_cols = cleaned_df.select_dtypes(include=['float64', 'int64']).columns
            for col in numeric_cols:
                if col in ['value_usd_m', 'market_size_usd_b', 'investment_potential_usd_m']:
                    Q1 = cleaned_df[col].quantile(0.25)
                    Q3 = cleaned_df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    upper_bound = Q3 + 1.5 * IQR
                    cleaned_df[col] = cleaned_df[col].clip(upper=upper_bound)
                    logger.info(f"Capped outliers in {dataset_name}.{col} at {upper_bound:.2f}")
        
        # Normalize text fields if categorical issues or missing values exist
        if cleaning_needed['categorical_issues'] or cleaning_needed['missing_values']:
            for col in cleaned_df.select_dtypes(include=['object']).columns:
                cleaned_df[col] = cleaned_df[col].str.strip().str.lower()
            logger.info(f"Normalized text fields in {dataset_name}")
        
        # Save cleaned data only if changes were made
        if any(cleaning_needed.values()):
            output_path = os.path.join(self.output_dir, f"cleaned_{dataset_name}.csv")
            cleaned_df.to_csv(output_path, index=False)
            logger.info(f"Saved cleaned data to {output_path}")
        else:
            logger.info(f"No cleaning needed for {dataset_name}, skipping save")
        
        return cleaned_df

    def run_checks_and_clean(self):
        """Run checks and clean all structured data where necessary."""
        try:
            print("\n=== Structured Data Issues ===")
            for dataset in self.structured_files:
                dataset_name = os.path.basename(dataset).replace('.csv', '')
                try:
                    df = pd.read_csv(dataset)
                    issues, cleaning_needed = self.check_structured_data(df, dataset_name)
                    if not issues:
                        print(f"No issues found in {dataset_name}")
                    else:
                        print(f"Issues in {dataset_name}:")
                        for issue in issues:
                            print(f"- {issue}")
                
                    # Clean data only where necessary
                    cleaned_df = self.clean_structured_data(df, dataset_name, cleaning_needed)
                    print(f"Processed {dataset_name}: {len(cleaned_df)} rows")
                except Exception as e:
                    logger.error(f"Error processing {dataset_name}: {e}")
                    print(f"Error processing {dataset_name}. Check logs.")
        
        except Exception as e:
            logger.error(f"Error in run_checks_and_clean: {e}")
            print("An error occurred. Check logs.")

def main():
    """Main function to check and clean AEC structured data."""
    csv_folder = "CSVs"
    
    try:
        cleaner = AECDataCleaner(csv_folder)
        cleaner.run_checks_and_clean()
        
        print("\n=== Cleaning Summary ===")
        print(f"Cleaned data (if necessary) saved in {cleaner.output_dir}/")
        print(f"Processed {len(cleaner.structured_files)} CSV files")
        print("Generated on: 11:15 PM +0545, Sunday, June 1, 2025")
    
    except FileNotFoundError as e:
        logger.error(str(e))
        print(str(e))

if __name__ == "__main__":
    main()
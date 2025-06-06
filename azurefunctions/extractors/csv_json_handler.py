import logging
import pandas as pd
import json
from io import StringIO, BytesIO
import chardet
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_text(text):
    """Remove extra whitespace and special characters."""
    try:
        logger.info("Starting text preprocessing")
        text = re.sub(r'\s+', ' ', text.strip())
        text = re.sub(r'[^\w\s.,-]', '', text)
        logger.info(f"Preprocessed text to {len(text)} characters")
        return text
    except Exception as e:
        logger.error(f"Text preprocessing failed: {str(e)}")
        raise

def extract_csv(blob_data):
    """
    Extract and preprocess data from a CSV blob.
    Handles various encodings and malformed files.
    Args:
        blob_data (bytes): CSV file content.
    Returns:
        dict: Preprocessed text and table data.
    """
    try:
        logger.info("Starting CSV extraction")
        # Detect encoding
        result = chardet.detect(blob_data)
        encoding = result['encoding'] if result['encoding'] else 'utf-8'
        # Try reading with detected encoding and fallback options
        try:
            df = pd.read_csv(BytesIO(blob_data), encoding=encoding, engine='python', error_bad_lines=False)
        except Exception as e:
            logger.warning(f"Primary CSV read failed: {str(e)}. Trying fallback with utf-8 and delimiter guess.")
            try:
                df = pd.read_csv(BytesIO(blob_data), encoding='utf-8', sep=None, engine='python', error_bad_lines=False)
            except Exception as e2:
                logger.error(f"CSV fallback read failed: {str(e2)}")
                raise

        df = df.fillna('')  # Handle missing values
        text = clean_text(df.to_string())
        table = df.to_dict(orient="records")
        logger.info(f"Extracted {len(table)} rows from CSV")
        return {"text": text, "table": table, "type": "csv"}
    except Exception as e:
        logger.error(f"CSV extraction failed: {str(e)}")
        raise

def extract_json(blob_data):
    """
    Extract and preprocess data from a JSON blob.
    Handles bytes and string input, and malformed JSON.
    Args:
        blob_data (bytes): JSON file content.
    Returns:
        dict: Preprocessed text and data.
    """
    try:
        logger.info("Starting JSON extraction")
        # Decode bytes if needed
        if isinstance(blob_data, bytes):
            try:
                blob_data = blob_data.decode('utf-8')
            except UnicodeDecodeError:
                encoding = chardet.detect(blob_data)['encoding'] or 'utf-8'
                blob_data = blob_data.decode(encoding)
        data = json.loads(blob_data)
        text = clean_text(json.dumps(data, indent=2))
        logger.info(f"Extracted JSON with {len(data) if isinstance(data, dict) else 'unknown'} items")
        return {"text": text, "data": data, "type": "json"}
    except Exception as e:
        logger.error(f"JSON extraction failed: {str(e)}")
        raise
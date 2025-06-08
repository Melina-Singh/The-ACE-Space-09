import logging
import re
import chardet

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_text(text):
    """Remove extra whitespace and special characters, keep Unicode."""
    try:
        logger.info("Starting text preprocessing")
        text = re.sub(r'\s+', ' ', text.strip())
        text = re.sub(r'[^\w\s.,\-–—:;!?()\'\"%€$£]', '', text, flags=re.UNICODE)
        logger.info(f"Preprocessed text to {len(text)} characters")
        return text
    except Exception as e:
        logger.error(f"Text preprocessing failed: {str(e)}")
        raise

def extract_text(blob_data, chunk_size=10000):
    """
    Extract and preprocess text from a plain text blob, with encoding detection and chunking.
    
    Args:
        blob_data (bytes): Text file content.
        chunk_size (int): Number of characters per chunk.
    
    Returns:
        dict: List of text chunks.
    """
    try:
        logger.info("Starting text extraction")
        result = chardet.detect(blob_data)
        encoding = result['encoding'] if result['encoding'] else 'utf-8'
        text = blob_data.decode(encoding)
        text = clean_text(text)
        text_chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]
        logger.info(f"Extracted {len(text)} characters in {len(text_chunks)} chunks from text file")
        return {"text_chunks": text_chunks, "type": "text"}
    except Exception as e:
        logger.error(f"Text extraction failed: {str(e)}")
        raise
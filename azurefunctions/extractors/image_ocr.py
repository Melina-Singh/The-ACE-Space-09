import logging
import pytesseract
from PIL import Image
import io
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_text(text):
    """Remove extra whitespace and special characters, keep Unicode."""
    try:
        text = re.sub(r'\s+', ' ', text.strip())
        text = re.sub(r'[^\w\s.,\-–—:;!?()\'\"%€$£]', '', text, flags=re.UNICODE)
        return text
    except Exception as e:
        logger.error(f"Text preprocessing failed: {str(e)}")
        raise

def extract_image_ocr(blob_data):
    """
    Extract and preprocess text and tables from an image blob using OCR.
    Args:
        blob_data (bytes): Image file content.
    Returns:
        dict: Extracted text and tables (if any).
    """
    try:
        logger.info("Starting image OCR extraction")
        image = Image.open(io.BytesIO(blob_data))
        # Extract text using pytesseract
        raw_text = pytesseract.image_to_string(image)
        text = clean_text(raw_text)

        # Attempt to extract tables using pytesseract's data frame output
        table_data = []
        try:
            ocr_df = pytesseract.image_to_data(image, output_type=pytesseract.Output.DATAFRAME)
            # Heuristic: group words by line and columns to form table-like structures
            if not ocr_df.empty:
                lines = ocr_df.groupby('line_num')
                for _, line in lines:
                    row = [str(word).strip() for word in line['text'] if str(word).strip()]
                    if row:
                        table_data.append(row)
        except Exception as e:
            logger.warning(f"Table extraction from image failed: {str(e)}")

        logger.info(f"Extracted {len(text)} characters and {len(table_data)} table rows from image")
        return {"text": text, "tables": table_data, "type": "image"}
    except Exception as e:
        logger.error(f"Image OCR extraction failed: {str(e)}")
        raise
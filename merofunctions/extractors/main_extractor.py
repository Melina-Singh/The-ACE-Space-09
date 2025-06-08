import logging
from .pdf_extractor import extract_pdf
from .docx_extractor import extract_docx
from .image_ocr import extract_image_ocr
from .csv_json_handler import extract_csv, extract_json
from .text_handler import extract_text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_file(blob_data, blob_name, doc_intel_endpoint=None, doc_intel_key=None):
    """
    Extract and preprocess content from a blob based on file extension.
    
    Args:
        blob_data (bytes): File content.
        blob_name (str): Blob name (e.g., 'aec_Data/market research/file1.pdf').
        doc_intel_endpoint (str): Document Intelligence endpoint.
        doc_intel_key (str): Document Intelligence key.
    
    Returns:
        dict: Preprocessed data.
    """
    try:
        logger.info(f"Processing file: {blob_name}")
        extension = blob_name.lower().split('.')[-1]
        
        if extension == 'pdf':
            if not doc_intel_endpoint or not doc_intel_key:
                raise ValueError("Document Intelligence endpoint and key required for PDF")
            return extract_pdf(blob_data, doc_intel_endpoint, doc_intel_key)
        elif extension == 'docx':
            return extract_docx(blob_data)
        elif extension in ('jpg', 'jpeg', 'png', 'bmp', 'svg'):
            return extract_image_ocr(blob_data)  # Updated to match function name
        elif extension == 'csv':
            return extract_csv(blob_data)
        elif extension == 'json':
            return extract_json(blob_data)
        elif extension == 'txt':
            return extract_text(blob_data)
        else:
            logger.error(f"Unsupported file extension: {extension}")
            raise ValueError(f"Unsupported file type: {extension}")
    
    except Exception as e:
        logger.error(f"File extraction failed for {blob_name}: {str(e)}")
        raise
import logging
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_text(text):
    """Remove extra whitespace and special characters, keep Unicode."""
    try:
        logger.info("Starting text preprocessing")
        text = re.sub(r'\s+', ' ', text.strip())  # Normalize whitespace
        # Allow Unicode letters, numbers, spaces, and basic punctuation
        text = re.sub(r'[^\w\s.,\-–—:;!?()\'\"%€$£]', '', text, flags=re.UNICODE)
        logger.info(f"Preprocessed text to {len(text)} characters")
        return text
    except Exception as e:
        logger.error(f"Text preprocessing failed: {str(e)}")
        raise

def extract_pdf(blob_data, endpoint, key, chunk_size=10000):
    """
    Extract and preprocess text/tables from a PDF blob in chunks.

    Args:
        blob_data (bytes): PDF file content.
        endpoint (str): Document Intelligence endpoint.
        key (str): Document Intelligence API key.
        chunk_size (int): Number of characters per text chunk.

    Returns:
        dict: List of text chunks and tables.
    """
    try:
        logger.info("Starting PDF extraction")
        client = DocumentIntelligenceClient(endpoint, AzureKeyCredential(key))
        poller = client.begin_analyze_document("prebuilt-layout", blob_data)
        result = poller.result()

        # Extract and preprocess all text
        full_text = "".join([paragraph.content for paragraph in getattr(result, "paragraphs", [])])
        full_text = clean_text(full_text)

        # Split text into chunks of chunk_size
        text_chunks = [full_text[i:i+chunk_size] for i in range(0, len(full_text), chunk_size)]

        # Extract and preprocess all tables (no limit)
        tables = []
        for table in getattr(result, "tables", []):
            table_data = {
                "row_count": table.row_count,
                "column_count": table.column_count,
                "cells": [
                    {
                        "content": clean_text(cell.content),
                        "row_index": cell.row_index,
                        "column_index": cell.column_index
                    }
                    for cell in table.cells
                ]
            }
            tables.append(table_data)

        logger.info(f"Extracted {len(full_text)} characters in {len(text_chunks)} chunks and {len(tables)} tables from PDF")
        return {"text_chunks": text_chunks, "tables": tables, "type": "pdf"}

    except Exception as e:
        logger.error(f"PDF extraction failed: {str(e)}")
        raise
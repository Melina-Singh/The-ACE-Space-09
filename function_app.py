import logging
import azure.functions as func
from azure.storage.blob import BlobClient
from azure.cosmos import CosmosClient
from extractors.main_extractor import extract_file
from openai import AzureOpenAI
import os
import requests
import time

app = func.FunctionApp()

def get_chunks(extracted_data):
    """
    Normalize extracted data to always return a list of text chunks.
    """
    chunks = extracted_data.get("text_chunks")
    if chunks:
        return chunks
    text = extracted_data.get("text", "")
    return [text] if text else []

def safe_upsert(container, doc, max_retries=3, delay=2):
    """
    Upsert item into Cosmos DB with retry logic for throttling.
    """
    for attempt in range(max_retries):
        try:
            container.upsert_item(doc)
            return
        except Exception as e:
            if "Request rate is large" in str(e) or "429" in str(e):
                logging.warning(f"Cosmos DB throttling, retrying ({attempt+1}/{max_retries})...")
                time.sleep(delay)
            else:
                logging.error(f"Cosmos DB upsert failed: {str(e)}")
                raise
    logging.error("Cosmos DB upsert failed after retries.")
    raise Exception("Cosmos DB upsert failed after retries.")

def call_openai_embedding(client, chunk, max_retries=3, delay=2):
    """
    Call OpenAI embedding API with retry logic for rate limits.
    """
    for attempt in range(max_retries):
        try:
            response = client.embeddings.create(model="text-embedding-ada-002", input=chunk)
            return response.data[0].embedding
        except Exception as e:
            if "429" in str(e) or "Rate limit" in str(e):
                logging.warning(f"OpenAI rate limit hit, retrying ({attempt+1}/{max_retries})...")
                time.sleep(delay)
            else:
                logging.error(f"OpenAI embedding failed: {str(e)}")
                raise
    logging.error("OpenAI embedding failed after retries.")
    raise Exception("OpenAI embedding failed after retries.")

def azure_ner(text, max_retries=3, delay=1):
    endpoint = os.environ["AZURE_LANGUAGE_ENDPOINT"]
    key = os.environ["AZURE_LANGUAGE_KEY"]
    url = f"{endpoint}/language/:analyze-text?api-version=2023-04-01-preview"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Content-Type": "application/json"
    }
    body = {
        "kind": "EntityRecognition",
        "parameters": {
            "modelVersion": "latest"
        },
        "analysisInput": {
            "documents": [
                {"id": "1", "language": "en", "text": text}
            ]
        }
    }
    for attempt in range(max_retries):
        response = requests.post(url, headers=headers, json=body)
        if response.ok:
            entities = []
            result = response.json()
            for ent in result["results"]["documents"][0]["entities"]:
                entities.append({"text": ent["text"], "label": ent["category"]})
            return entities
        elif response.status_code == 429:
            logging.warning("Azure NER API rate limit hit. Retrying after delay...")
            time.sleep(delay)
        else:
            logging.error(f"Azure NER API error: {response.text}")
            break
    return []

@app.event_grid_trigger(arg_name="azeventgrid")
def triggerevent(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')

    blob_url = azeventgrid.get_json()['data']['url']
    blob_name = blob_url.split('/')[-1]
    category = blob_url.split('/')[-3]
    logging.info(f"Processing blob: {blob_name}")

    # Initialize clients
    blob_client = BlobClient.from_blob_url(blob_url)
    cosmos_client = CosmosClient(
        os.environ["COSMOS_ENDPOINT"],
        os.environ["COSMOS_KEY"]
    )
    database = cosmos_client.get_database_client("aecDataDB")
    container = database.get_container_client("ProcessedData")
    openai_client = AzureOpenAI(
        api_key=os.environ["OPENAI_KEY"],
        api_version="2023-05-15",
        azure_endpoint=os.environ["OPENAI_ENDPOINT"]
    )

    # Download blob
    try:
        blob_data = blob_client.download_blob().readall()
        logging.info(f"Downloaded blob: {blob_name}")
    except Exception as e:
        logging.error(f"Blob download failed: {str(e)}")
        raise

    # Extract and preprocess
    try:
        extracted_data = extract_file(
            blob_data,
            blob_name,
            doc_intel_endpoint=os.environ.get("DOC_INTEL_ENDPOINT"),
            doc_intel_key=os.environ.get("DOC_INTEL_KEY")
        )
        logging.info(f"Extracted data from {blob_name}")
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")
        raise

    # Consistent chunk handling
    chunks = get_chunks(extracted_data)

    # Embed, apply Azure NER, and store in Cosmos DB
    for i, chunk in enumerate(chunks):
        try:
            # Apply Azure NER to chunk with retry logic
            entities = azure_ner(chunk)

            # Embed chunk with retry logic
            embedding = call_openai_embedding(openai_client, chunk)

            # Prepare Cosmos DB document (avoid logging or storing full text/embedding in logs)
            cosmos_doc = {
                "id": f"{blob_name.replace('/', '_')}_{i}",
                "category": category,
                "filename": blob_name,
                # "chunk": chunk,  # Uncomment only if you need to store the chunk
                "embedding": embedding,  # Do not log this
                "entities": entities,
                "chunk_index": i,
                "data": None,  # Optionally remove or minimize sensitive extracted_data
                "timestamp": azeventgrid.event_time.isoformat()
            }

            # Store in Cosmos DB with retry logic
            safe_upsert(container, cosmos_doc)
            logging.info(f"Saved chunk {i} for {blob_name} to Cosmos DB")

        except Exception as e:
            logging.error(f"Processing failed for chunk {i} of {blob_name}: {str(e)}")
            continue
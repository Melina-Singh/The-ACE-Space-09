import logging
import azure.functions as func
import json
import os
import time
from datetime import datetime

# Initialize the Function App with proper configuration
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

def get_chunks(extracted_data):
    """
    Normalize extracted data to always return a list of text chunks.
    """
    try:
        chunks = extracted_data.get("text_chunks")
        if chunks:
            return chunks
        text = extracted_data.get("text", "")
        return [text] if text else []
    except Exception as e:
        logging.error(f"Error getting chunks: {str(e)}")
        return []

def safe_upsert(container, doc, max_retries=3, delay=2):
    """
    Upsert item into Cosmos DB with retry logic for throttling.
    """
    for attempt in range(max_retries):
        try:
            container.upsert_item(doc)
            logging.info(f"Successfully upserted document: {doc.get('id', 'unknown')}")
            return
        except Exception as e:
            error_msg = str(e)
            if "Request rate is large" in error_msg or "429" in error_msg:
                logging.warning(f"Cosmos DB throttling, retrying ({attempt+1}/{max_retries})...")
                time.sleep(delay * (attempt + 1))  # Exponential backoff
            else:
                logging.error(f"Cosmos DB upsert failed: {error_msg}")
                raise
    
    error_msg = "Cosmos DB upsert failed after all retries"
    logging.error(error_msg)
    raise Exception(error_msg)

def create_openai_client():
    """
    Create OpenAI client with error handling for version compatibility issues.
    """
    try:
        from openai import AzureOpenAI
        return AzureOpenAI(
            api_key=os.environ["OPENAI_KEY"],
            api_version="2023-05-15",
            azure_endpoint=os.environ["OPENAI_ENDPOINT"]
        )
    except TypeError as e:
        if "proxies" in str(e) or "unexpected keyword argument" in str(e):
            logging.warning(f"OpenAI client initialization failed with version compatibility issue: {str(e)}")
            logging.warning("This is likely due to OpenAI SDK version incompatibility. ")
            raise Exception("OpenAI client initialization failed due to version incompatibility.")
        else:
            raise
    except Exception as e:
        logging.error(f"Failed to create OpenAI client: {str(e)}")
        raise

def call_openai_embedding(client, chunk, max_retries=3, delay=2):
    """
    Call OpenAI embedding API with retry logic for rate limits.
    """
    if not chunk or not chunk.strip():
        logging.warning("Empty chunk provided for embedding")
        return []
    
    for attempt in range(max_retries):
        try:
            response = client.embeddings.create(
                model="my-embedding-model", 
                input=chunk[:8000]  # Limit input length
            )
            return response.data[0].embedding
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "Rate limit" in error_msg:
                logging.warning(f"OpenAI rate limit hit, retrying ({attempt+1}/{max_retries})...")
                time.sleep(delay * (attempt + 1))  # Exponential backoff
            else:
                logging.error(f"OpenAI embedding failed: {error_msg}")
                raise
    
    error_msg = "OpenAI embedding failed after all retries"
    logging.error(error_msg)
    raise Exception(error_msg)

def azure_ner(text, max_retries=3, delay=1):
    """
    Extract named entities using Azure Language Service with retry logic.
    """
    try:
        import requests
        
        endpoint = os.environ.get("AZURE_LANGUAGE_ENDPOINT")
        key = os.environ.get("AZURE_LANGUAGE_KEY")
        
        if not endpoint or not key:
            logging.warning("Azure Language Service credentials not found, skipping NER")
            return []
        
        url = f"{endpoint}/language/:analyze-text?api-version=2023-04-01"
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
                    {"id": "1", "language": "en", "text": text[:5000]}  # Limit text length
                ]
            }
        }
        
        last_error = None
        for attempt in range(max_retries):
            try:
                response = requests.post(url, headers=headers, json=body, timeout=30)
                if response.ok:
                    entities = []
                    result = response.json()
                    documents = result.get("results", {}).get("documents", [])
                    if documents:
                        for ent in documents[0].get("entities", []):
                            entities.append({"text": ent["text"], "label": ent["category"]})
                    return entities
                elif response.status_code == 429:
                    logging.warning(f"Azure NER API rate limit hit. Retrying after delay... ({attempt+1}/{max_retries})")
                    time.sleep(delay * (attempt + 1))
                    last_error = f"Rate limit: {response.status_code}"
                else:
                    last_error = f"HTTP {response.status_code}: {response.text}"
                    logging.error(f"Azure NER API error: {last_error}")
                    break
            except requests.RequestException as e:
                last_error = str(e)
                logging.error(f"Azure NER request failed: {last_error}")
                if attempt < max_retries - 1:
                    time.sleep(delay)
        
        # If we have credentials but still failed, raise exception
        if endpoint and key:
            raise Exception(f"Azure NER failed after all retries: {last_error}")
        
        return []
        
    except Exception as e:
        logging.error(f"Azure NER failed: {str(e)}")
        # Re-raise if we had credentials (this is a real failure)
        if os.environ.get("AZURE_LANGUAGE_ENDPOINT") and os.environ.get("AZURE_LANGUAGE_KEY"):
            raise
        return []

def process_blob(blob_url, event_time):
    """
    Shared logic to process a single blob (used by both triggers).
    """
    try:
        from urllib.parse import urlparse
        from azure.storage.blob import BlobClient, BlobServiceClient
        from azure.cosmos import CosmosClient
        from merofunctions.extractors.main_extractor import extract_file
        
        # Parse blob URL
        parsed_url = urlparse(blob_url)
        container_and_blob = parsed_url.path.lstrip('/').split('/', 1)
        if len(container_and_blob) != 2:
            raise Exception(f"Invalid blob URL format: {blob_url}")
        
        container_name = container_and_blob[0]
        blob_path = container_and_blob[1]
        blob_name = blob_path.split('/')[-1]  # e.g., dile.csv, file.json
        # Set category to container name (mydataset1) since no virtual folders
        category = container_name
        
        logging.info(f"Processing blob: {blob_name} in category: {category}, container: {container_name}")

        # Validate required environment variables
        required_env_vars = ["COSMOS_ENDPOINT", "COSMOS_KEY", "OPENAI_KEY", "OPENAI_ENDPOINT", "AzureWebJobsStorage"]
        missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
        if missing_vars:
            raise Exception(f"Missing environment variables: {', '.join(missing_vars)}")

        # Initialize clients
        connection_string = os.environ.get("AzureWebJobsStorage")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        
        cosmos_client = CosmosClient(
            os.environ["COSMOS_ENDPOINT"],
            os.environ["COSMOS_KEY"]
        ) 
        database = cosmos_client.get_database_client("ProcessedDB")
        container = database.get_container_client("aecDataDB")
        openai_client = create_openai_client()

        # Download blob
        try:
            blob_data = blob_client.download_blob().readall()
            logging.info(f"Downloaded blob: {blob_name} ({len(blob_data)} bytes)")
        except Exception as e:
            logging.error(f"Blob download failed for {blob_name}: {str(e)}")
            raise

        # Extract and preprocess
        try:
            extracted_data = extract_file(
                blob_data,
                blob_name,
                doc_intel_endpoint=os.environ.get("DOC_INTEL_ENDPOINT"),
                doc_intel_key=os.environ.get("DOC_INTEL_KEY")
            )
            logging.info(f"Successfully extracted data from {blob_name}")
        except Exception as e:
            logging.error(f"Extraction failed for {blob_name}: {str(e)}")
            raise

        # Get chunks consistently
        chunks = get_chunks(extracted_data)
        if not chunks:
            error_msg = f"No chunks extracted from {blob_name}"
            logging.error(error_msg)
            raise Exception(error_msg)

        logging.info(f"Processing {len(chunks)} chunks for {blob_name}")

        # Process each chunk
        successful_chunks = 0
        chunk_errors = []
        
        for i, chunk in enumerate(chunks):
            try:
                if not chunk or not chunk.strip():
                    error_msg = f"Empty chunk {i} for {blob_name}"
                    logging.warning(error_msg)
                    chunk_errors.append(error_msg)
                    continue

                entities = azure_ner(chunk)
                embedding = call_openai_embedding(openai_client, chunk)
                cosmos_doc = {
                    "id": f"{blob_name.replace('/', '_').replace('.', '_')}_{i}",
                    "category": category,
                    "filename": blob_name,
                    "text": chunk,
                    "embedding": embedding,
                    "entities": entities,
                    "chunk_index": i,
                    "chunk_length": len(chunk),
                    "timestamp": event_time,
                    "processed_at": datetime.utcnow().isoformat() + "Z"
                }
                safe_upsert(container, cosmos_doc)
                successful_chunks += 1
                logging.info(f"Successfully processed chunk {i}/{len(chunks)} for {blob_name}")

            except Exception as e:
                error_msg = f"Failed to process chunk {i} of {blob_name}: {str(e)}"
                logging.error(error_msg)
                chunk_errors.append(error_msg)
                continue

        failure_rate = (len(chunks) - successful_chunks) / len(chunks)
        if successful_chunks == 0:
            error_msg = f"All chunks failed for {blob_name}. Errors: {'; '.join(chunk_errors[:3])}"
            logging.error(error_msg)
            raise Exception(error_msg)
        elif failure_rate > 0.5:
            error_msg = f"High failure rate for {blob_name}: {successful_chunks}/{len(chunks)} successful. Errors: {'; '.join(chunk_errors[:3])}"
            logging.warning(error_msg)

        logging.info(f"Completed processing {blob_name}: {successful_chunks}/{len(chunks)} chunks successful")
        
    except Exception as e:
        logging.error(f"Critical error processing blob {blob_url}: {str(e)}")
        raise

# Event Grid trigger for new blobs
@app.event_grid_trigger(arg_name="azeventgrid")
def triggerevent(azeventgrid: func.EventGridEvent):
    """
    Process new blobs via Event Grid trigger.
    """
    try:
        logging.info('Python EventGrid trigger processing an event')
        
        event_data = azeventgrid.get_json()
        blob_url = event_data.get('data', {}).get('url')
        
        if not blob_url:
            logging.error("No blob URL found in event data")
            return
            
        event_time = azeventgrid.event_time.isoformat() if azeventgrid.event_time else datetime.utcnow().isoformat() + "Z"
        
        process_blob(blob_url, event_time)
        logging.info(f"Successfully processed event for blob: {blob_url}")
        
    except Exception as e:
        logging.error(f"EventGrid trigger failed: {str(e)}")
        raise

# HTTP trigger for processing existing blobs
@app.route(route="process_existing", methods=["GET", "POST"])
def process_existing_blobs(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger to process all existing blobs in the specified container.
    Also handles Event Grid webhook validation.
    """
    try:
        # Handle Event Grid webhook validation for POST requests
        if req.method == "POST":
            try:
                req_body = req.get_json()
                
                # Check if this is an Event Grid validation request
                if req_body and isinstance(req_body, list) and len(req_body) > 0:
                    event = req_body[0]
                    
                    if event.get('eventType') == 'Microsoft.EventGrid.SubscriptionValidationEvent':
                        validation_code = event['data']['validationCode']
                        logging.info(f'Event Grid validation code: {validation_code}')
                        
                        # Return the validation response
                        response_data = {
                            'validationResponse': validation_code
                        }
                        
                        return func.HttpResponse(
                            json.dumps(response_data),
                            status_code=200,
                            mimetype="application/json"
                        )
                    
                    # Handle actual Event Grid blob events if needed
                    elif event.get('eventType') in ['Microsoft.Storage.BlobCreated', 'Microsoft.Storage.BlobDeleted']:
                        logging.info(f'Processing Event Grid blob event: {event.get("eventType")}')
                        blob_url = event['data']['url']
                        event_time = event.get('eventTime', datetime.utcnow().isoformat() + "Z")
                        
                        process_blob(blob_url, event_time)
                        
                        return func.HttpResponse(
                            json.dumps({"status": "success", "message": "Event processed"}),
                            status_code=200,
                            mimetype="application/json"
                        )
            
            except (ValueError, TypeError):
                # If JSON parsing fails, continue with normal processing
                pass
        
        # Original functionality for processing existing blobs
        logging.info('Processing existing blobs via HTTP trigger')
        
        # Import here to avoid module loading issues
        from azure.storage.blob import BlobServiceClient
        
        # Get parameters
        container_name = req.params.get('container', 'mydataset1')
        
        # Validate environment variables
        connection_string = os.environ.get("AzureWebJobsStorage")
        if not connection_string:
            return func.HttpResponse(
                json.dumps({"error": "AzureWebJobsStorage connection string not found"}),
                status_code=500,
                mimetype="application/json"
            )

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        processed_count = 0
        failed_count = 0
        blob_errors = []
        
        # Get list of blobs first
        try:
            blob_list = list(container_client.list_blobs())
            if not blob_list:
                return func.HttpResponse(
                    json.dumps({"error": f"No blobs found in container '{container_name}'"}),
                    status_code=404,
                    mimetype="application/json"
                )
            
            logging.info(f"Found {len(blob_list)} blobs to process in container '{container_name}'")
        except Exception as e:
            return func.HttpResponse(
                json.dumps({"error": f"Failed to list blobs in container '{container_name}': {str(e)}"}),
                status_code=500,
                mimetype="application/json"
            )
        
        # Process blobs
        for blob in blob_list:
            try:
                blob_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob.name}"
                process_blob(blob_url, datetime.utcnow().isoformat() + "Z")
                processed_count += 1
                logging.info(f"Processed blob {processed_count}: {blob.name}")
                
            except Exception as e:
                failed_count += 1
                error_msg = f"Failed to process blob {blob.name}: {str(e)}"
                logging.error(error_msg)
                blob_errors.append(error_msg)
                continue

        # Check if we had too many failures
        total_blobs = len(blob_list)
        failure_rate = failed_count / total_blobs
        
        result = {
            "status": "completed" if failure_rate < 0.5 else "completed_with_errors",
            "processed_count": processed_count,
            "failed_count": failed_count,
            "total_blobs": total_blobs,
            "container": container_name,
            "errors": blob_errors[:5] if blob_errors else []  # Show first 5 errors
        }
        
        if processed_count == 0 and failed_count > 0:
            # All blobs failed
            logging.error(f"All {total_blobs} blobs failed to process")
            return func.HttpResponse(
                json.dumps({**result, "error": "All blobs failed to process"}),
                status_code=500,
                mimetype="application/json"
            )
        elif failure_rate > 0.8:  # More than 80% failed
            logging.error(f"High failure rate: {failed_count}/{total_blobs} failed")
            return func.HttpResponse(
                json.dumps({**result, "warning": "High failure rate"}),
                status_code=206,  # Partial success
                mimetype="application/json"
            )
        
        logging.info(f"Batch processing completed: {processed_count} successful, {failed_count} failed")
        
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        error_msg = f"Error processing existing blobs: {str(e)}"
        logging.error(error_msg)
        return func.HttpResponse(
            json.dumps({"error": error_msg}),
            status_code=500,
            mimetype="application/json"
        )

# Health check endpoint
@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Simple health check endpoint to verify the function app is running.
    """
    try:
        # Check if required environment variables exist
        required_vars = ["COSMOS_ENDPOINT", "COSMOS_KEY", "OPENAI_KEY", "OPENAI_ENDPOINT"]
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        health_status = {
            "status": "healthy" if not missing_vars else "degraded",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "missing_env_vars": missing_vars
        }
        
        return func.HttpResponse(
            json.dumps(health_status),
            status_code=200 if not missing_vars else 206,
            mimetype="application/json"
        )
        
    except Exception as e:
        return func.HttpResponse(
            json.dumps({"status": "unhealthy", "error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
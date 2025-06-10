import azure.functions as func
import json
import logging
import os
import asyncio
from typing import List, Dict, Any
from azure.search.documents.aio import SearchClient
from azure.search.documents.models import VectorizedQuery
from azure.core.credentials import AzureKeyCredential
from openai import AzureOpenAI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = func.FunctionApp()

class RAGService:
    def __init__(self):
        # Initialize OpenAI client
        self.openai_client = AzureOpenAI(
            azure_endpoint=os.getenv("OPENAI_ENDPOINT"),
            api_key=os.getenv("OPENAI_KEY"),
            api_version=os.getenv("OPENAI_API_VERSION")
        )
        
        # Initialize Search client
        self.search_client = SearchClient(
            endpoint=os.getenv("SEARCH_ENDPOINT"),
            index_name=os.getenv("SEARCH_INDEX_NAME"),
            credential=AzureKeyCredential(os.getenv("SEARCH_API_KEY"))
        )
        
        self.embedding_model = os.getenv("EMBEDDING_MODEL")
        self.chat_model = os.getenv("OPENAI_DEPLOYMENT_NAME")
    
    async def get_embedding(self, text: str) -> List[float]:
        """Generate embedding for text"""
        try:
            response = self.openai_client.embeddings.create(
                model=self.embedding_model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            raise
    
    async def search_documents(self, query: str, query_embedding: List[float], top_k: int = 5) -> List[Dict]:
        """Search documents using hybrid search"""
        try:
            vector_query = VectorizedQuery(
                vector=query_embedding,
                k_nearest_neighbors=top_k,
                fields="content_vector"
            )
            
            results = await self.search_client.search(
                search_text=query,
                vector_queries=[vector_query],
                top=top_k,
                select=["id", "content", "title", "metadata"]
            )
            
            documents = []
            async for result in results:
                documents.append({
                    "id": result.get("id"),
                    "content": result.get("content", ""),
                    "title": result.get("title", ""),
                    "metadata": result.get("metadata", {}),
                    "score": result.get("@search.score", 0)
                })
            
            return documents
            
        except Exception as e:
            logger.error(f"Error searching documents: {e}")
            raise
    
    def create_context(self, documents: List[Dict]) -> str:
        """Create context from retrieved documents"""
        context_parts = []
        for i, doc in enumerate(documents, 1):
            title = doc.get("title", f"Document {i}")
            content = doc.get("content", "")
            context_parts.append(f"[Source {i}] {title}:\n{content}")
        
        return "\n\n".join(context_parts)
    
    async def generate_response(self, query: str, context: str) -> Dict[str, Any]:
        """Generate response using context"""
        system_prompt = """You are a helpful AI assistant that answers questions based on the provided context. 
        Use only the information from the context to answer questions. If the context doesn't contain 
        enough information to answer the question, say so clearly. Always cite your sources by referencing 
        the source numbers in your response."""
        
        user_prompt = f"""Context:
{context}

Question: {query}

Please provide a comprehensive answer based on the context above."""
        
        try:
            response = self.openai_client.chat.completions.create(
                model=self.chat_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=1000,
                temperature=0.3
            )
            
            return {
                "response": response.choices[0].message.content,
                "usage": {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating response: {e}")
            raise
    
    async def query(self, question: str, top_k: int = 5) -> Dict[str, Any]:
        """Main RAG query method"""
        try:
            # Generate embedding
            query_embedding = await self.get_embedding(question)
            
            # Search documents
            documents = await self.search_documents(question, query_embedding, top_k)
            
            if not documents:
                return {
                    "answer": "I couldn't find relevant information to answer your question.",
                    "sources": [],
                    "retrieved_documents": 0
                }
            
            # Create context
            context = self.create_context(documents)
            
            # Generate response
            generation_result = await self.generate_response(question, context)
            
            # Format sources
            sources = [
                {
                    "id": doc.get("id"),
                    "title": doc.get("title"),
                    "score": doc.get("score")
                }
                for doc in documents
            ]
            
            return {
                "answer": generation_result["response"],
                "sources": sources,
                "retrieved_documents": len(documents),
                "usage": generation_result["usage"]
            }
            
        except Exception as e:
            logger.error(f"Error in RAG query: {e}")
            raise

# Initialize RAG service
rag_service = RAGService()

@app.route(route="rag", methods=["POST"])
async def rag_query(req: func.HttpRequest) -> func.HttpResponse:
    """RAG Query HTTP Trigger"""
    
    logger.info('RAG query function processed a request.')
    
    try:
        # Parse request body
        req_body = req.get_json()
        
        if not req_body:
            return func.HttpResponse(
                json.dumps({"error": "Request body is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        question = req_body.get('question', '').strip()
        
        if not question:
            return func.HttpResponse(
                json.dumps({"error": "Question field is required"}),
                status_code=400,
                mimetype="application/json"
            )
        
        # Optional parameters
        top_k = req_body.get('top_k', 5)
        
        # Validate top_k
        if not isinstance(top_k, int) or top_k <= 0:
            top_k = 5
        
        # Process RAG query
        result = await rag_service.query(question=question, top_k=top_k)
        
        return func.HttpResponse(
            json.dumps(result, default=str),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logger.error(f"Error processing RAG query: {e}")
        return func.HttpResponse(
            json.dumps({"error": "Internal server error"}),
            status_code=500,
            mimetype="application/json"
        )

@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint"""
    return func.HttpResponse(
        json.dumps({"status": "healthy", "service": "RAG Pipeline"}),
        status_code=200,
        mimetype="application/json"
    )
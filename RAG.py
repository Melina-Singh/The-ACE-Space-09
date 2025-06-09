import os
import numpy as np
from dotenv import load_dotenv
from openai import OpenAI
from azure.cosmos import CosmosClient
from sklearn.metrics.pairwise import cosine_similarity

# Load environment variables
load_dotenv()

# Init clients
openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
cosmos = CosmosClient(os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY"))
db = cosmos.get_database_client(os.getenv("COSMOS_DB_NAME"))
container = db.get_container_client(os.getenv("COSMOS_CONTAINER_NAME"))

# Step 1: Get user question
user_query = input("Ask your question: ")

# Step 2: Get embedding for the question
response = openai.embeddings.create(
    model="text-embedding-3-small",
    input=user_query
)
query_vector = response.data[0].embedding

# Step 3: Read documents from Cosmos DB
documents = list(container.read_all_items())
document_vectors = [doc["embedding"] for doc in documents]

# Step 4: Calculate similarity
similarities = cosine_similarity([query_vector], document_vectors)[0]
top_k_indices = similarities.argsort()[-3:][::-1]  # Top 3
top_docs = [documents[i] for i in top_k_indices]

# Step 5: Build prompt
context = "\n\n".join(doc["content"] for doc in top_docs)
prompt = f"""Answer the question using the following context:

{context}

Question: {user_query}
"""

# Step 6: Generate answer with GPT
chat_response = openai.chat.completions.create(
    model="gpt-4",
    messages=[{"role": "user", "content": prompt}]
)
print("\n🤖 Answer:")
print(chat_response.choices[0].message.content.strip())

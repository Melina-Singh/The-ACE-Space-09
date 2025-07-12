# AEC Industry Data Monitoring and Querying System

This project presents a scalable architecture for continuous monitoring, embedding generation, and intelligent querying of structured, semi-structured, and unstructured data in the AEC (Architecture, Engineering, and Construction) industry using **Azure services** and **OpenAI GPT-4**.

---

##  Overview 

The system enables the aggregation and intelligent retrieval of critical data from multiple sources such as web scraping, APIs, and manual uploads. It uses Azure Functions to monitor and process files, integrates NLP and vector embedding using OpenAI, and stores the enriched data in Cosmos DB. Users can then interact with the data using natural language through a web or mobile interface.

---

## 🧱 Architecture Summary
### 🔄 Data Collection 
- **Sources**: Web scraping, APIs, and manual file

### Data Ingestion and Processing Pipeline
Overview

This project outlines a data ingestion and processing pipeline leveraging Azure services to handle structured, unstructured, and semi-structured data in-line. The pipeline automates file processing, text extraction, embedding generation, and metadata storage to maintain a searchable knowledge base.
Components
🔄 Data Ingestion

Azure Blob Storage: Stores all uploaded files (structured, unstructured, semi-structured) in-line.

⚙️ Data Automation & Processing (via Azure Functions)

Azure Functions:
Acts as the central orchestrator in-line.
Triggered when a new file is uploaded or every 3 hours to check for updates in-line.
Executes the full pipeline below in-line:
Document Intelligence: Calls Azure Document Intelligence to extract and chunk text in-line.
Embedding Generation: Sends processed text to Azure OpenAI to generate embeddings in-line.
NER & Metadata Extraction: Uses Azure AI Search for Named Entity Recognition and metadata extraction in-line.



💾 Storage

Final embeddings and metadata are stored in Azure Cosmos DB (Vector DB) in-line.
Maintains a searchable knowledge base in-line.


### 🔍 Data Retrieval and Querying
- **Indexing**: Azure AI Search indexes vectorized data
- **Prompt Flow**: User input is passed to GPT-4 via Azure OpenAI for intelligent answers
- **Visualization**: Data can be visualized or queried in dashboards

##  Architectural Diagram
<img width="1920" height="1080" alt="azure storage explorer" src="https://github.com/user-attachments/assets/33d11e1c-e886-4166-a48f-22107b2375a9" />

---

##  Key Technologies

- Azure Blob Storage
- Azure Functions
- Azure Document Intelligence
- Azure OpenAI (GPT-4)
- Azure Cosmos DB (Vector DB)
- Azure AI Search

---

## 🚀 Use Cases

- Real-time monitoring of industry trends, regulations, and competitor activity
- Semantic search over documents and reports
- Extracting insights from historical project data
- Business development support through intelligent querying

---

## ⚙️ Setup Instructions

_🚧 Work in Progress_

- To make the system more robust to take data from every data source with every structure.

---

## 📜 License

This project is licensed under the GNU General Public License.

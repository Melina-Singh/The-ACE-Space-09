# AEC Industry Data Monitoring and Querying System

This project presents a scalable architecture for continuous monitoring, embedding generation, and intelligent querying of structured, semi-structured, and unstructured data in the AEC (Architecture, Engineering, and Construction) industry using **Azure services** and **OpenAI GPT-4**.

---

##  Overview

The system enables the aggregation and intelligent retrieval of critical data from multiple sources such as web scraping, APIs, and manual uploads. It uses Azure Functions to monitor and process files, integrates NLP and vector embedding using OpenAI, and stores the enriched data in Cosmos DB. Users can then interact with the data using natural language through a web or mobile interface.

---

## 🧱 Architecture Summary

### 🔄 Data Ingestion
- **Sources**: Web scraping, APIs, and manual file uploads
- **Azure Blob Storage**: Stores all files (structured, unstructured, semi-structured)
- **Azure Functions**: Triggered when a new file is uploaded to process the file

### ⚙️ Data Processing Pipeline
- **Document Intelligence**: Uses Azure Document Intelligence to extract and chunk text
- **Embedding Generation**: Embeddings are created using Azure OpenAI models
- **NER & Metadata Extraction**: Azure AI Search performs Named Entity Recognition and metadata extraction
- **Storage**: All outputs are stored in **Azure Cosmos DB** as a **Vector DB**

### 🔍 Data Retrieval and Querying
- **Indexing**: Azure AI Search indexes vectorized data
- **Prompt Flow**: User input is passed to GPT-4 via Azure OpenAI for intelligent answers
- **Visualization**: Data can be visualized or queried in dashboards

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

- Clone this repository
- Configure Azure resources (Blob, Cosmos DB, Functions, OpenAI, Search)
- Set environment variables and connection strings
- Deploy Azure Functions and test the data ingestion pipeline

Full instructions coming soon.

---

## 📊 Demo

_🚧 Coming Soon_  
Live demo link and screenshots will be added here.

---

## 🤝 Contributing

Contributions are welcome! Please open an issue or pull request for feature suggestions, bug fixes, or improvements.

---

## 📜 License

This project is licensed under the [MIT License](LICENSE).

---

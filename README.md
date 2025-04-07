# Data Pipeline for Business Analytics

A data engineering project that implements an ETL pipeline, data lake, data warehouse, vector database, and REST API for movies business analysis.

## Architecture
- **PostgreSQL**: Hosts the Gold layer in a data warehouse with a star schema, storing fact and dimension tables (e.g., `fact_movie_metrics`, `dim_movie`) for efficient querying.
- **Typesense**: Vector database providing fast search capabilities for movie entities, synced with the Gold layer.
- **FastAPI**: REST API for data seeding, querying, and (partially implemented) CRUD operations.
- **Data Lake**: Medallion architecture with:
  - **Bronze Layer**: Raw, unprocessed data stored as Parquet files (e.g., `bronze_movies.parquet`) and original uploaded files (e.g., CSV, JSON), preserved as-is for lineage and auditing.
  - **Silver Layer**: Cleaned, deduplicated data stored as Parquet files (e.g., `silver_movies.parquet`), processing only new, unique records.
  - **Gold Layer**: Structured data in a star schema, stored in PostgreSQL, with lineage tracking.

### Architecture Overview

This diagram illustrates the high-level data flow through the movies data pipeline:

```mermaid
graph TD
    A[FastAPI Routes] --> B[Controllers]
    B --> C[Services]
    C --> D[Data Lake: Bronze & Silver as Parquet]
    C --> E[PostgreSQL: Gold Layer]
    C --> F[Typesense]
    G[Uploaded Files] --> A
```
## Dataset

This project uses the "IMDB movies dataset" from Kaggle, available at: https://www.kaggle.com/datasets/ashpalsingh1525/imdb-movies-dataset

## Project Structure

This project is a movie data pipeline built with FastAPI, PostgreSQL, and Typesense. Click below to view the directory structure:

### Key Design Notes:
- **Incremental Processing:** Only new data is processed from bronze to silver and gold. Existing movies in `bronze_movies.parquet` are not reprocessed unless updated via a PUT operation.
- **Deduplication in Silver:** The silver layer deduplicates data based on `name` and `orig_title`, ensuring only unique records flow to gold. Original files in bronze remain untouched.
- **Data Flow:** Data moves from bronze (raw) -> silver (cleaned, deduplicated) -> gold (structured star schema in PostgreSQL), with Typesense syncing from gold for search.

<summary>View Directory Structure</summary>

```plaintext
src/
├── movies_data_pipeline/           # Main app directory
│   ├── api/                        # FastAPI API layer
│   │   ├── routes/                 # API endpoints
│   │   │   ├── crud.py             # CRUD routes (partial)
│   │   │   ├── gold.py             # GET /revenue_by_genre, GET /avg_score_by_year
│   │   │   ├── search.py           # GET /search/
│   │   │   └── seed.py             # POST /
│   │   └── main.py                 # FastAPI entry point
│   ├── controllers/                # Business logic
│   │   ├── crud_controller.py      # CRUD operations (partial)
│   │   ├── gold_controller.py      # Gold layer queries
│   │   ├── search_controller.py    # Search logic
│   │   └── seed_controller.py      # Seeding logic
│   ├── services/                   # Data processing services
│   │   ├── bronze_service.py       # Bronze layer operations
│   │   ├── etl_service.py          # ETL pipeline coordinator
│   │   ├── extractor_service.py    # Data extraction
│   │   ├── transformer_service.py  # Data transformation
│   │   ├── loader_service.py       # Data loading
│   │   ├── search_service.py       # Search logic
│   │   ├── search_service_adapter.py # Typesense integration
│   │   └── initialize_service.py   # Schema initialization
│   ├── data_access/                # Data layer
│   │   ├── data_lake/              # Data lake
│   │   │   ├── bronze/             # Raw data (Parquet + original files)
│   │   │   ├── silver/             # Cleaned data (Parquet)
│   │   │   └── gold/               # (Not used; gold in PostgreSQL)
│   │   ├── models/                 # SQLModel models
│   │   │   └── gold.py             # Gold layer table definitions
│   │   ├── database.py             # PostgreSQL connection
│   │   └── vector_db.py            # Typesense connection
│   ├── domain/                     # Domain logic
│   │   └── models/                 # Pydantic models
│   │       ├── bronze.py           # BronzeMovieUpdate model
│   │       └── movie.py            # Movie entity
├── .env                            # Enviroment variables
├── Dockerfile                      # FastAPI Dockerfile
├── Dockerfile.typesense            # Typesense Dockerfile
├── docker-compose.yml              # Docker Compose setup
├── pyproject.toml                  # Poetry dependencies
└── README.md                       # Documentation
```
### Overview of Key Components
| Directory         | Purpose                              |
|-------------------|--------------------------------------|
| `api/`            | FastAPI routes for seeding, querying, and partial CRUD. |
| `controllers/`    | Logic for API endpoints.           |
| `services/`       | ETL pipeline, bronze operations, and search services. |
| `data_access/`    | Data lake, PostgreSQL, and Typesense connections. |
| `domain/`         | Pydantic-defined domain entities.      |

##  Architecture Details

### The ETL pipeline is modular and follows the medallion architecture:

- **Extractor:** Ingests raw data into the bronze layer as bronze_movies.parquet, appending new records with sequential bronze_ids. Original files (e.g., CSV, JSON) are stored in the bronze directory unchanged.
- **Transformer:** Processes only new data from bronze to silver, deduplicating based on name and orig_title. Transforms silver data into gold-layer star schema tables.
- **Loader:** Loads gold tables into PostgreSQL, mapping lineage IDs to database-generated IDs, with a lightweight deduplication safety net.
- **SearchServiceAdapter:** Syncs Typesense with the gold layer for search functionality.
- **ETLService:** Orchestrates the pipeline, processing only new files and triggering Typesense sync.
- **BronzeService:** Handles bronze operations (seeding, partial CRUD), ensuring updates occur only via explicit PUT requests.

### Deduplication Strategy:

- Bronze retains all raw data, including duplicates, for auditing and lineage.
- Silver deduplicates incrementally, processing only new records and keeping existing ones unless updated via PUT.
- Gold inherits deduplicated data, structured into a star schema in PostgreSQL.


## Data Lake Structure

- **Bronze Layer:** Raw data stored as `bronze_movies.parquet`, with original files (e.g., `imdb_movies_20250406.csv`) retained in the same directory.
- **Silver Layer:** Cleaned, deduplicated data as `silver_movies.parquet`, with fields like `genre_list` and `crew_pairs` processed for gold.
- **Gold Layer (PostgreSQL):** Star schema with: 
  - **Fact Table:** `fact_movie_metrics` (e.g., `movie_id`, `budget`, `revenue`, `score`).
  - **Dimension Tables:** `dim_movie`, `dim_date`, `dim_country`, `dim_language`, `dim_crew`, `dim_genre`.
  - **Bridge Tables:** `bridge_movie_genre`, `bridge_movie_crew` for many-to-many relationships.
  - **Aggregates:** `revenue_by_genre`, `avg_score_by_year`.
  - **Lineage:** `lineage_log` for tracking transformations.


## REST API Endpoints

- **POST** `/seed`: Uploads a file (CSV, JSON, PDF) to seed data into bronze, appending to `bronze_movies.parquet` and storing the original file with a timestamp.
- GET** `/search/`: Searches movies in Typesense by query and optional genre filter, with pagination (`limit`, `offset`).
- GET** `/revenue_by_genre`: Retrieves total revenue by genre from PostgreSQL.
- GET** `/avg_score_by_year`: Retrieves average score by year from PostgreSQL.
- GET** `/data/`: Fetches paginated bronze data (implemented).
- GET** `/files/`: Lists bronze files excluding `bronze_movies.parquet` (implemented).
- PUT** `/data/`: Updates bronze records (partial, in progress).
- DELETE** `/data/{bronze_id}`: Deletes a bronze record (partial, in progress).
- POST** `/data/`: Creates new bronze records (single JSON object or an array of JSON objects)

Note: This is a first commit; full CRUD operations (`POST /data/`, complete `PUT /data/`, `DELETE /data/{bronze_id}`) are still in development.



## Setup and Running

- **Docker**: Use `docker-compose.yml` to run PostgreSQL, Typesense, and the FastAPI app.
- **Dependencies**: Managed via `pyproject.toml` with Poetry.

### Clone the repository:
```bash
git clone <repository-url>
cd <repository-directory>
```
### Build and start the services:
```bash
docker-compose up --build
```
### Access the services:

- FastAPI: http://localhost:8000
- PostgreSQL: accessible via SSH or pgAdmin (see below)

### Accessing the PostgreSQL Database

The Gold layer data in PostgreSQL can be accessed in two ways: via SSH into the postgres container or through an optional pgAdmin web interface. The pgAdmin service is included in docker-compose.yml but commented out by default for flexibility.

## Option 1: SSH into the PostgreSQL Container
### Start the services (if not already running):
```bash
docker-compose up -d
```
### Access the postgres container:
```bash
docker exec -it postgres_db bash
```
### Connect to the PostgreSQL database:
```bash
psql -U admin -d gold
```
### Use SQL commands to explore the database, e.g.:
```bash
\dt             -- List tables
SELECT * FROM revenue_by_genre;  -- View data
```
## Option 2: Use pgAdmin (Optional)
Open `docker-compose.yml` and uncomment the pgadmin service section:
```
# pgadmin:
#   image: dpage/pgadmin4:latest
#   container_name: pgadmin
#   environment:
#     PGADMIN_DEFAULT_EMAIL: admin@admin.com
#     PGADMIN_DEFAULT_PASSWORD: admin
#     PGADMIN_LISTEN_PORT: 80
#   ports:
#     - "8080:80"
#   depends_on:
#     postgres:
#       condition: service_healthy
#   volumes:
#     - pgadmin_data:/var/lib/pgadmin
#   networks:
#     - app_network
volumes:
  postgres_data:
  typesense_data:
  # pgadmin_data:
```
## Start the services:
```bash
docker-compose up -d --build
```
Open pgAdmin in your browser at http://localhost:8080.

**Log in with:**

- Email: admin@admin.com
- Password: admin

**Add a new server:**

**General Tab:**

- Name: postgres (or any name)

**Connection Tab:**

- Host: postgres (Docker service name)

- Port: 5432

- Database: gold

- Username: admin

- Password: password

## Project Status

This is the **first commit** for the Movies Data Pipeline, fulfilling core requirements from the technical challenge:
- Medallion architecture with bronze (raw), silver (deduplicated), and gold (star schema) layers.
- Data lake using Parquet files for bronze and silver.
- Data warehouse in PostgreSQL with 1 fact table, 6+ dimension tables, and bridge tables.
- Typesense vector DB via Docker for fast search.
- FastAPI REST API with seeding and querying implemented, partial CRUD in progress.
- Pydantic domain models and SQLModel for gold tables.

**Next Steps:**

- Complete CRUD endpoints ( finalize PUT /data/, DELETE /data/{bronze_id}).
- Add authentication (e.g., Basic Auth or JWT).
- Include Mermaid diagrams for each layer (bronze, silver, gold) and overall data lake.
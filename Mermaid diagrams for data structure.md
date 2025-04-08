## Diagram for the Bronze Layer

The Bronze layer is the raw data layer, storing unprocessed data as `bronze_movies.parquet` and retaining original uploaded files (e.g., `CSV`, `JSON`, `parquet`) in the same directory for auditing and lineage.

```mermaid
graph TD
    A["Uploaded Files (CSV, JSON, parquet)"] -->|"POST /bronze/"| B["BronzeService"]
    B -->|"Saves Original File"| C["Bronze Directory (/data_lake/bronze/)"]
    B -->|"Appends to"| D["bronze_movies.parquet"]
    C -.->|"Contains"| D
    D -->|"Raw Data with bronze_id"| E["Next Layer: Silver"]
```
### Explanation:

- **Uploaded Files**: Users upload files via the `/bronze/` endpoint.
- **BronzeService**: Handles the ingestion, saving the original file and appending data to `bronze_movies.parquet`.
- **Bronze Directory**: Stores both the original files and `bronze_movies.parquet`.
 - **Flow**: Data is ingested, stored as-is, and prepared for the next layer (Silver).

## Diagram for the Silver Layer

The Silver layer processes data from the Bronze layer, performing cleaning and deduplication based on `name` and `orig_title`, and stores the result as `silver_movies.parquet`.

```mermaid
graph TD
    A["bronze_movies.parquet"] -->|"ETLService"| B["Transformer"]
    B -->|"Standardize Columns"| C["Cleaned Data"]
    B -->|"Process Dates"| C
    B -->|"Process Genre & Crew"| C
    C -->|"Deduplicate<br/>(name, orig_title)"| D["silver_movies.parquet"]
    D -->|"Cleaned, Deduplicated Data"| E["Next Layer: Gold"]
```

## Explanation:

- **bronze_movies.parquet**: The raw data from the Bronze layer.
- **ETLService/Transformer**: Processes the data by standardizing columns, processing dates, and handling genres and crew.
- **Deduplication**: Ensures uniqueness based on `name` and `orig_title`.
- **silver_movies.parquet**: Stores the cleaned, deduplicated data, ready for the Gold layer.

## Diagram for the Gold Layer

The Gold layer structures the data into a star schema in PostgreSQL, with fact, dimension, and bridge tables, and syncs with Typesense for search.

```mermaid
graph TD
    A["silver_movies.parquet"] -->|"ETLService"| B["Transformer"]
    B -->|"Create Star Schema"| C["PostgreSQL: Gold Layer"]
    C --> D["fact_movie_metrics<br/>(movie_id, budget, revenue, score)"]
    C --> E["dim_movie"]
    C --> F["dim_date"]
    C --> G["dim_country"]
    C --> H["dim_language"]
    C --> I["dim_crew"]
    C --> J["dim_genre"]
    C --> K["bridge_movie_genre"]
    C --> L["bridge_movie_crew"]
    C --> M["lineage_log"]
    C --> N["Data Mart"]
    N --> O["dm_revenue_by_genre_year"]
    N --> P["dm_top_movies_by_revenue"]
    N -->|"Query"| Q["GET /datamart/revenue_by_genre_year (JWT-protected)"]
    N -->|"Query"| R["GET /datamart/top_movies_by_revenue (JWT-protected)"]
    C -->|"Sync"| S["Typesense<br/>(Vector DB)"]
    S -->|"Search"| T["GET /search/ (JWT-protected)"]
    D -->|"Query"| U["GET /revenue_by_genre (JWT-protected)"]
    D -->|"Query"| V["GET /avg_score_by_year (JWT-protected)"]
```

## Explanation:

- **silver_movies.parquet**: The cleaned data from the Silver layer.
- **ETLService/Transformer**: Transforms the data into a star schema.
- **PostgreSQL (Gold Layer)**: Stores the structured data in fact, dimension, and bridge tables.
- **Typesense**: Syncs with the Gold layer for search functionality.
- **Queries**: The Gold layer supports analytical queries and search endpoints.

## Diagram for the Overall Data Lake Organization
This diagram shows the organization and flow across all layers of the data lake, including the interaction with external systems (PostgreSQL, Typesense).

```mermaid
graph TD
    A["Uploaded Files<br/>(CSV, JSON, parquet)"] -->|"POST /seed/ (JWT-protected)"| B["Bronze Layer"]
    B -->|"Raw Data"| C["bronze_movies.parquet"]
    B -->|"Original Files"| D["Bronze Directory<br/>(/data_lake/bronze/)"]
    C -->|"ETLService"| E["Silver Layer"]
    E -->|"Cleaned, Deduplicated"| F["silver_movies.parquet"]
    F -->|"ETLService"| G["Gold Layer"]
    G -->|"Star Schema"| H["PostgreSQL"]
    H --> I["fact_movie_metrics"]
    H --> J["Dimensions & Bridges"]
    H --> K["lineage_log"]
    H --> L["Data Mart"]
    L --> M["dm_revenue_by_genre_year"]
    L --> N["dm_top_movies_by_revenue"]
    L -->|"Query"| O["GET /datamart/revenue_by_genre_year (JWT-protected)"]
    L -->|"Query"| P["GET /datamart/top_movies_by_revenue (JWT-protected)"]
    H -->|"Sync"| Q["Typesense"]
    Q -->|"Search"| R["GET /search/ (JWT-protected)"]
    H -->|"Query"| S["GET /revenue_by_genre (JWT-protected)"]
    H -->|"Query"| T["GET /avg_score_by_year (JWT-protected)"]
    C -->|"CRUD"| U["BronzeService"]
    U -->|"GET /bronze/v1/data/ (JWT-protected)"| V["Paginated Data"]
    U -->|"GET /bronze/v2/data/ (JWT-protected)"| W["By bronze_id or name"]
    U -->|"GET /bronze/v1/files/ (JWT-protected)"| X["List Files"]
    U -->|"POST /bronze/v1/data/ (JWT-protected)"| Y["Create"]
    U -->|"PUT /bronze/v1/update-full-etl/ (JWT-protected)"| Z["Update"]
    U -->|"DELETE /bronze/v1/delete-full-etl/ (JWT-protected)"| AA["Delete"]
```

## Explanation:

- **Bronze Layer**: Stores raw data and original files.
- **Silver Layer**: Processes and deduplicates data.
- **Gold Layer**: Structures data in PostgreSQL and syncs with Typesense.
- **CRUD Operations**: `BronzeService` supports:
  - `/bronze/v1/data/` (GET) for paginated data.
  - `/v1/files/` (GET) to list files.
  - `/v1/data/` (POST) to create.
  - `/v1/update-full-etl/` (PUT) to update.
  - `/v1/delete-full-etl/` (DELETE) to delete.  
  - `/v2/data/` (GET) to fetch a single record by `bronze_id` or `name`.
- **Queries and Search**: The Gold layer supports analytical queries and search via Typesense.
All endpoints are now JWT-protected.


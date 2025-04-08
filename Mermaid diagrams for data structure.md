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
    N -->|"Query"| Q["GET /datamart/revenue_by_genre_year"]
    N -->|"Query"| R["GET /datamart/top_movies_by_revenue"]
    C -->|"Sync"| S["Typesense<br/>(Vector DB)"]
    S -->|"Search"| T["GET /search/"]
    D -->|"Query"| U["GET /revenue_by_genre"]
    D -->|"Query"| V["GET /avg_score_by_year"]
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
    A["Uploaded Files<br/>(CSV, JSON, parquet)"] -->|"POST /seed/"| B["Bronze Layer"]
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
    L -->|"Query"| O["GET /datamart/revenue_by_genre_year"]
    L -->|"Query"| P["GET /datamart/top_movies_by_revenue"]
    H -->|"Sync"| Q["Typesense"]
    Q -->|"Search"| R["GET /search/"]
    H -->|"Query"| S["GET /revenue_by_genre"]
    H -->|"Query"| T["GET /avg_score_by_year"]
    C -->|"CRUD"| U["BronzeService"]
    U -->|"GET /data/"| V["Paginated Data"]
    U -->|"GET /files/"| W["List Files"]
    U -->|"POST /data/"| X["Create"]
    U -->|"PUT /bronze/update-full-etl/"| Y["Update"]
    U -->|"DELETE /bronze/delete-full-etl/"| Z["Delete"]
```

## Explanation:

- **Bronze Layer**: Stores raw data and original files.
- **Silver Layer**: Processes and deduplicates data.
- **Gold Layer**: Structures data in PostgreSQL and syncs with Typesense.
- **CRUD Operations**: The `BronzeService` supports full CRUD on the Bronze layer, with updates and deletes triggering full ETL (truncate-and-load).
- **Queries and Search**: The Gold layer supports analytical queries and search via Typesense.












from sqlalchemy import create_engine, Table, MetaData, text
import logging
from sqlalchemy.orm import Session
from movies_data_pipeline.data_access.vector_db import VectorDB
from movies_data_pipeline.data_access.database import get_session_direct
from typing import Dict
import pandas as pd

logger = logging.getLogger(__name__)


class TruncateLoader:
    def __init__(self, silver_file_path: str, db_engine):
        """Initialize the TruncateLoader with silver file path and database engine."""
        self.silver_file_path = silver_file_path
        self.db_engine = db_engine
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.db_engine)

    def load_gold(self, gold_tables: Dict[str, pd.DataFrame]):
        """Truncate gold tables and load new data from gold_tables."""
        dimension_tables = [
            "dim_movie",
            "dim_date",
            "dim_country",
            "dim_language",
            "dim_crew",
            "dim_genre",
        ]
        bridge_fact_tables = [
            "bridge_movie_genre",
            "bridge_movie_crew",
            "fact_movie_metrics",
            "revenue_by_genre",
            "avg_score_by_year",
        ]
        other_tables = ["lineage_log"]

        try:
            with self.db_engine.connect() as conn:
                with conn.begin():
                    # Truncate all tables using text()
                    for table_name in (
                        dimension_tables + bridge_fact_tables + other_tables
                    ):
                        if table_name in self.metadata.tables:
                            conn.execute(
                                text(
                                    f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"
                                )
                            )
                            logger.info(f"Truncated table {table_name}")

                    # Load dimension tables
                    id_mappings = {}
                    for table_name in dimension_tables:
                        if (
                            table_name in gold_tables
                            and not gold_tables[table_name].empty
                        ):
                            df = gold_tables[table_name]
                            table = Table(
                                table_name, self.metadata, autoload_with=self.db_engine
                            )
                            db_columns = [c.name for c in table.columns]
                            df = df[[col for col in df.columns if col in db_columns]]
                            records = df.to_dict("records")
                            conn.execute(table.insert().values(records))
                            logger.info(f"Loaded {table_name} with {len(df)} records")
                            id_col = table.primary_key.columns.values()[0].name
                            mapping_df = pd.read_sql(
                                f"SELECT {id_col}, lineage_id FROM {table_name}", conn
                            )
                            id_mappings[table_name] = dict(
                                zip(mapping_df["lineage_id"], mapping_df[id_col])
                            )

                    # Load bridge and fact tables with updated foreign keys
                    for table_name in bridge_fact_tables:
                        if (
                            table_name in gold_tables
                            and not gold_tables[table_name].empty
                        ):
                            df = gold_tables[table_name]
                            table = Table(
                                table_name, self.metadata, autoload_with=self.db_engine
                            )
                            db_columns = [c.name for c in table.columns]
                            if table_name == "bridge_movie_genre":
                                df["movie_id"] = df["movie_lineage_id"].map(
                                    id_mappings["dim_movie"]
                                )
                                df["genre_id"] = df["genre_lineage_id"].map(
                                    id_mappings["dim_genre"]
                                )
                                df = df[
                                    [
                                        "movie_id",
                                        "genre_id",
                                        "lineage_id",
                                        "created_at",
                                        "updated_at",
                                    ]
                                ]
                            elif table_name == "bridge_movie_crew":
                                df["movie_id"] = df["movie_lineage_id"].map(
                                    id_mappings["dim_movie"]
                                )
                                df["crew_id"] = df["crew_lineage_id"].map(
                                    id_mappings["dim_crew"]
                                )
                                df = df[
                                    [
                                        "movie_id",
                                        "crew_id",
                                        "character_name",
                                        "lineage_id",
                                        "created_at",
                                        "updated_at",
                                    ]
                                ]
                            elif table_name == "fact_movie_metrics":
                                df["movie_id"] = df["movie_lineage_id"].map(
                                    id_mappings["dim_movie"]
                                )
                                df["date_id"] = df["date_lineage_id"].map(
                                    id_mappings["dim_date"]
                                )
                                df["country_id"] = df["country_lineage_id"].map(
                                    id_mappings["dim_country"]
                                )
                                df["language_id"] = df["language_lineage_id"].map(
                                    id_mappings["dim_language"]
                                )
                                df = df[
                                    [
                                        "silver_id",
                                        "movie_id",
                                        "date_id",
                                        "country_id",
                                        "language_id",
                                        "budget",
                                        "revenue",
                                        "score",
                                        "lineage_id",
                                        "created_at",
                                        "updated_at",
                                    ]
                                ]
                            df = df[[col for col in df.columns if col in db_columns]]
                            records = df.to_dict("records")
                            conn.execute(table.insert().values(records))
                            logger.info(f"Loaded {table_name} with {len(df)} records")

                    # Load lineage log
                    for table_name in other_tables:
                        if (
                            table_name in gold_tables
                            and not gold_tables[table_name].empty
                        ):
                            df = gold_tables[table_name]
                            table = Table(
                                table_name, self.metadata, autoload_with=self.db_engine
                            )
                            db_columns = [c.name for c in table.columns]
                            df = df[[col for col in df.columns if col in db_columns]]
                            records = df.to_dict("records")
                            conn.execute(table.insert().values(records))
                            logger.info(f"Loaded {table_name} with {len(df)} records")

                logger.info("Finished truncate-and-load, syncing with Typesense")
                try:
                    with Session(self.db_engine) as session:
                        vector_db = VectorDB(initialize=False, db_session=session)
                        logger.info("Initializing sync with gold layer")
                        vector_db._sync_with_gold()
                        logger.info("Typesense sync completed")
                except Exception as sync_e:
                    logger.error(f"Error during Typesense sync: {str(sync_e)}")
                    raise

        except Exception as e:
            logger.error(f"Error during truncate-and-load: {str(e)}")
            raise

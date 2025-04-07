# movies_data_pipeline/services/etl_service.py
import pandas as pd
from typing import Dict, Union, List, Any
import logging
from sqlalchemy import create_engine
from .extractor_service import Extractor  # Assuming this is your Extractor class
from .transformer_service import Transformer
from .loader_service import Loader
from .truncate_loader_service import TruncateLoader
from .search_service_adapter import SearchServiceAdapter
from movies_data_pipeline.data_access.vector_db import VectorDB
from movies_data_pipeline.domain.models.bronze import BronzeMovieUpdate
import os
from pathlib import Path

logger = logging.getLogger(__name__)

class ETLService:
    def __init__(self):
        self.bronze_file_path = Path(os.getenv("BRONZE_BASE_PATH")) / "bronze_movies.parquet"
        self.silver_file_path = Path(os.getenv("SILVER_BASE_PATH")) / "silver_movies.parquet"
        self.db_engine = create_engine(os.getenv("DATABASE_URL"))
        
        self.extractor = Extractor(self.bronze_file_path)
        self.transformer = Transformer(self.bronze_file_path, self.silver_file_path)
        self.loader = Loader(self.silver_file_path, self.db_engine)
        self.truncate_loader = TruncateLoader(self.silver_file_path, self.db_engine)
        self.search_adapter = SearchServiceAdapter(self.bronze_file_path)
        self.vector_db = VectorDB(initialize=False)

    def transform(self, new_file_path: str) -> Dict[str, pd.DataFrame]:
        logger.info("Starting transform phase")
        transformed_data = self.transformer.transform(new_file_path)
        logger.info("Transform phase completed")
        return transformed_data

    def load(self, gold_tables: Dict[str, pd.DataFrame]) -> None:
        logger.info("Starting load phase")
        self.loader.load_gold(gold_tables)
        logger.info("Load phase completed")

    def update_and_run_full_etl(self, updates: Union[Dict[str, Any], List[Dict[str, Any]]]) -> int:
        from movies_data_pipeline.services.bronze_service import BronzeService
        try:
            update_list = [updates] if isinstance(updates, dict) else updates
            for update in update_list:
                if "bronze_id" not in update:
                    raise ValueError("Each update must include a 'bronze_id'")
            bronze_updates = [BronzeMovieUpdate(**update) for update in update_list]
            bronze_service = BronzeService(str(self.bronze_file_path), self)
            bronze_service.update_bronze(bronze_updates)
            self._update_silver_with_bronze_ids([update.bronze_id for update in bronze_updates])
            self._run_full_etl()
            return len(update_list)
        except Exception as e:
            logger.error(f"Failed in update_and_run_full_etl: {str(e)}")
            raise

    def delete_and_run_full_etl(self, bronze_ids: Union[int, List[int]]) -> int:
        from movies_data_pipeline.services.bronze_service import BronzeService
        try:
            bronze_id_list = [bronze_ids] if isinstance(bronze_ids, int) else bronze_ids
            bronze_service = BronzeService(str(self.bronze_file_path), self)
            for bronze_id in bronze_id_list:
                bronze_service.delete_bronze(bronze_id)
            self._delete_from_silver(bronze_id_list)
            self._run_full_etl()
            return len(bronze_id_list)
        except Exception as e:
            logger.error(f"Failed in delete_and_run_full_etl: {str(e)}")
            raise

    def _update_silver_with_bronze_ids(self, bronze_ids: List[int]):
        try:
            if not self.bronze_file_path.exists():
                raise ValueError("Bronze file does not exist")
            bronze_df = pd.read_parquet(self.bronze_file_path)
            
            if not self.silver_file_path.exists():
                logger.info("Silver file does not exist; creating from bronze updates")
                silver_df = pd.DataFrame()
            else:
                silver_df = pd.read_parquet(self.silver_file_path)
            
            updated_bronze_df = bronze_df[bronze_df["bronze_id"].isin(bronze_ids)]
            if updated_bronze_df.empty:
                logger.info(f"No records found in bronze for bronze_ids {bronze_ids}")
                return
            
            updated_bronze_df = self.transformer._standardize_columns(updated_bronze_df)
            updated_bronze_df = self.transformer._process_dates(updated_bronze_df)
            updated_bronze_df = self.transformer._process_genre_and_crew(updated_bronze_df)
            
            if not silver_df.empty and "bronze_id" in silver_df.columns:
                for _, updated_row in updated_bronze_df.iterrows():
                    bronze_id = updated_row["bronze_id"]
                    mask = silver_df["bronze_id"] == bronze_id
                    if mask.any():
                        existing_silver_id = silver_df.loc[mask, "silver_id"].iloc[0]
                        for col in updated_row.index:
                            if col != "silver_id":
                                silver_df.loc[mask, col] = updated_row[col]
                    else:
                        max_silver_id = silver_df["silver_id"].max() if "silver_id" in silver_df.columns else 0
                        updated_row_df = pd.DataFrame([updated_row])
                        updated_row_df["silver_id"] = max_silver_id + 1
                        silver_df = pd.concat([silver_df, updated_row_df], ignore_index=True)
            else:
                max_silver_id = 0
                updated_bronze_df["silver_id"] = range(max_silver_id + 1, max_silver_id + 1 + len(updated_bronze_df))
                silver_df = updated_bronze_df
            
            unique_key = ["name", "orig_title"]
            silver_df = silver_df.drop_duplicates(subset=unique_key, keep="last")
            
            current_time = pd.Timestamp.now()
            silver_df["created_at"] = silver_df["created_at"].fillna(current_time)
            silver_df["updated_at"] = current_time
            
            silver_df.to_parquet(self.silver_file_path)
            logger.info(f"Updated silver layer with {len(updated_bronze_df)} records for bronze_ids {bronze_ids}")
        
        except Exception as e:
            logger.error(f"Failed to update silver: {str(e)}")
            raise

    def _delete_from_silver(self, bronze_ids: List[int]):
        try:
            if not self.silver_file_path.exists():
                logger.info("Silver file does not exist; nothing to delete")
                return
            
            silver_df = pd.read_parquet(self.silver_file_path)
            if silver_df.empty:
                logger.info("Silver file is empty; nothing to delete")
                return
            
            deleted_count = 0
            if "bronze_id" in silver_df.columns:
                mask = silver_df["bronze_id"].isin(bronze_ids)
                if mask.any():
                    silver_df = silver_df[~mask]
                    deleted_count += mask.sum()
            
            remaining_ids = [bid for bid in bronze_ids if bid not in silver_df["bronze_id"].values] if "bronze_id" in silver_df.columns else bronze_ids
            if remaining_ids:
                bronze_df = pd.read_parquet(self.bronze_file_path)
                bronze_records = bronze_df[bronze_df["bronze_id"].isin(remaining_ids)]
                if not bronze_records.empty:
                    for _, row in bronze_records.iterrows():
                        name = row["name"]
                        orig_title = row["orig_title"]
                        mask = (silver_df["name"] == name) & (silver_df["orig_title"] == orig_title)
                        if mask.any():
                            silver_df = silver_df[~mask]
                            deleted_count += mask.sum()
                            logger.info(f"Deleted record with name '{name}' and orig_title '{orig_title}' from silver")
                        else:
                            logger.info(f"No match in silver for bronze_id {row['bronze_id']}, name '{name}', orig_title '{orig_title}'")
                else:
                    logger.info(f"No records found in bronze for remaining bronze_ids {remaining_ids}")
            
            if deleted_count > 0:
                silver_df.to_parquet(self.silver_file_path)
                logger.info(f"Deleted {deleted_count} records from silver")
            else:
                logger.info(f"No records deleted from silver for bronze_ids {bronze_ids}")
        
        except Exception as e:
            logger.error(f"Failed to delete from silver: {str(e)}")
            raise

    def _run_full_etl(self):
        try:
            if not self.silver_file_path.exists():
                logger.info("Silver file does not exist; skipping ETL")
                return
            
            silver_df = pd.read_parquet(self.silver_file_path)
            if silver_df.empty:
                logger.info("Silver file is empty; skipping ETL")
                return
            
            current_time = pd.Timestamp.now()
            lineage_entries = [{
                "lineage_log_id": 1,
                "record_id": f"silver_update_{int(current_time.timestamp())}",
                "source_path": str(self.bronze_file_path),
                "stage": "silver",
                "transformation": f"updated_{len(silver_df)}_records",
                "timestamp": current_time
            }]
            
            gold_tables = self.transformer._create_gold_tables(silver_df, lineage_entries, str(self.bronze_file_path))
            self.truncate_loader.load_gold(gold_tables)
            logger.info("Full ETL with truncate-and-load completed")
        except Exception as e:
            logger.error(f"Full ETL failed: {str(e)}")
            raise
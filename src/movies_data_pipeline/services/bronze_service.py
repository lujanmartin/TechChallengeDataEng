import logging
from pathlib import Path
import pandas as pd
from typing import Tuple, List, Optional, Dict, List
from movies_data_pipeline.domain.models.bronze import BronzeMovieUpdate
import math
from fastapi import UploadFile
import os

logger = logging.getLogger(__name__)


class BronzeService:
    def __init__(self, bronze_file_path: str, etl_service):
        """Initialize the BronzeService with a bronze file path and ETLService."""
        self.etl_service = etl_service  # Directly assign the provided ETLService
        self.bronze_file_path = Path(bronze_file_path)

    def seed_bronze(self, file: UploadFile, temp_file_path: str) -> None:
        """Seed bronze data with an uploaded file and run ETL."""
        with open(temp_file_path, "wb") as f:
            f.write(file.file.read())
        self.process_bronze_data(temp_file_path)

    def process_bronze_data(self, file_path: str):
        """Append data to bronze and run ETL transformation and loading for the new file."""
        filename = Path(file_path).name
        try:
            records_appended = self.etl_service.extractor.append_to_bronze(file_path)
            if records_appended == 0:
                logger.info(f"No new records appended from {filename}; skipping ETL")
                return
            logger.info(f"Starting transformation for {filename}")
            gold_tables = self.etl_service.transform(file_path)
            if gold_tables:
                self.etl_service.load(gold_tables)
                logger.info(
                    f"ETL completed for {filename}: transformed and loaded {len(gold_tables)} tables"
                )
            else:
                logger.info(
                    f"No new unique data to transform from {filename}; ETL stopped after silver"
                )
        except Exception as e:
            logger.error(f"ETL processing failed for {filename}: {str(e)}")
            raise

    def get_bronze_paginated(
        self, page: int, page_size: int
    ) -> Tuple[pd.DataFrame, int, int, int, int]:
        """Fetch a paginated subset of bronze data with pagination metadata."""
        if not self.bronze_file_path.exists():
            logger.info("Bronze file does not exist; returning empty result")
            return pd.DataFrame(), page, page_size, 0, 0
        df = pd.read_parquet(self.bronze_file_path)
        total_records = len(df)
        total_pages = math.ceil(total_records / page_size)
        if page > total_pages:
            page = total_pages if total_pages > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_df = df.iloc[start_idx:end_idx]
        logger.info(
            f"Fetched {len(paginated_df)} records from bronze (page {page}, size {page_size})"
        )
        return paginated_df, page, page_size, total_records, total_pages

    def get_bronze_by_id_or_name(
        self, bronze_id: Optional[int] = None, name: Optional[str] = None
    ) -> pd.DataFrame:
        """Fetch a single bronze record by bronze_id or name."""
        if not self.bronze_file_path.exists():
            logger.info("Bronze file does not exist; returning empty result")
            return pd.DataFrame()

        df = pd.read_parquet(self.bronze_file_path)

        if bronze_id is not None:
            result = df[df["bronze_id"] == bronze_id]
            if result.empty:
                logger.info(f"No record found for bronze_id {bronze_id}")
                raise ValueError(f"No record found with bronze_id {bronze_id}")
        elif name is not None:
            result = df[df["name"].str.lower() == name.lower()]
            if result.empty:
                logger.info(f"No record found for name {name}")
                raise ValueError(f"No record found with name {name}")
        else:
            raise ValueError("Either bronze_id or name must be provided")

        logger.info(f"Fetched record for bronze_id={bronze_id} or name={name}")
        return result

    def delete_bronze(self, bronze_id: int) -> None:
        """Delete a bronze record by bronze_id and reprocess ETL."""
        if not self.bronze_file_path.exists():
            raise ValueError("Bronze data not found")

        df = pd.read_parquet(self.bronze_file_path)
        if bronze_id not in df["bronze_id"].values:
            raise ValueError(f"Record with bronze_id {bronze_id} not found")

        df = df[df["bronze_id"] != bronze_id]
        df.to_parquet(self.bronze_file_path)

        gold_tables = self.etl_service.transform(str(self.bronze_file_path))
        if gold_tables:
            self.etl_service.load(gold_tables)
        logger.info(
            f"Deleted bronze record with bronze_id {bronze_id} and triggered ETL"
        )

    def list_bronze_files(self) -> List[str]:
        """List all files in the bronze directory, excluding bronze_movies.parquet."""
        bronze_dir = self.bronze_file_path.parent
        if not bronze_dir.exists():
            logger.info(f"Bronze directory {bronze_dir} does not exist")
            return []

        files = [
            f.name
            for f in bronze_dir.iterdir()
            if f.is_file() and f.name != "bronze_movies.parquet"
        ]
        logger.info(f"Found {len(files)} files in bronze directory {bronze_dir}")
        return files

    def update_bronze(self, updates: List[BronzeMovieUpdate]) -> None:
        """Update bronze records in the parquet file."""
        try:
            if not self.bronze_file_path.exists():
                raise ValueError("Bronze file does not exist")

            df = pd.read_parquet(self.bronze_file_path)
            for update in updates:
                mask = df["bronze_id"] == update.bronze_id
                if not mask.any():
                    raise ValueError(
                        f"No record found with bronze_id {update.bronze_id}"
                    )

                # Use model_dump() and add type hint
                update_dict: Dict[str, Any] = update.model_dump()
                for key, value in update_dict.items():
                    # skip bronze_id as it's used for the mask and shouldn't be updated
                    if key != "bronze_id":
                        df.loc[mask, key] = value

            df.to_parquet(self.bronze_file_path)
            logger.info(f"Updated {len(updates)} records in {self.bronze_file_path}")
        except Exception as e:
            logger.error(f"Failed to update bronze: {str(e)}")
            raise

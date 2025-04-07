# movies_data_pipeline/crud_controller.py
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from typing import Dict, List, Union, Any
from movies_data_pipeline.services.bronze_service import BronzeService
from movies_data_pipeline.services.etl_service import ETLService
from movies_data_pipeline.data_access.database import get_db_engine
from movies_data_pipeline.domain.models.bronze import BronzeMovieUpdate
import logging
from pathlib import Path
import os
import pandas as pd

logger = logging.getLogger(__name__)

class CrudController:
    def __init__(self):
        self.router = APIRouter()
        self._register_routes()

    def get_etl_service(self) -> ETLService:
        return ETLService()

    def get_bronze_service(self, db_engine=Depends(get_db_engine), etl_service: ETLService = Depends(lambda: ETLService())) -> BronzeService:
        bronze_file_path = Path(os.getenv("BRONZE_BASE_PATH")) / "bronze_movies.parquet"
        return BronzeService(bronze_file_path, etl_service)

    def _register_routes(self):
        @self.router.get("/data/", response_model=Dict[str, Any])
        async def get_bronze_paginated(
            page: int = Query(1, ge=1),
            page_size: int = Query(10, ge=1, le=100),
            bronze_service: BronzeService = Depends(self.get_bronze_service)
        ):
            try:
                df, page, page_size, total_records, total_pages = bronze_service.get_bronze_paginated(page, page_size)
                return {
                    "data": df.to_dict(orient="records"),
                    "page": page,
                    "page_size": page_size,
                    "total_records": total_records,
                    "total_pages": total_pages
                }
            except Exception as e:
                logger.error(f"Failed to fetch paginated bronze data: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get("/files/", response_model=Dict[str, List[str]])
        async def list_bronze_files(
            bronze_service: BronzeService = Depends(self.get_bronze_service)
        ):
            try:
                files = bronze_service.list_bronze_files()
                return {"files": files}
            except Exception as e:
                logger.error(f"Failed to list bronze files: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.post("/data/", response_model=Dict[str, str])
        async def create_bronze_data(
            data: Union[Dict[str, Any], List[Dict[str, Any]]],
            background_tasks: BackgroundTasks,
            bronze_service: BronzeService = Depends(self.get_bronze_service)
        ):
            try:
                df = pd.DataFrame([data]) if isinstance(data, dict) else pd.DataFrame(data)
                if df.empty:
                    raise ValueError("No data provided")
                temp_file_path = f"/tmp/new_data_{int(pd.Timestamp.now().timestamp())}.parquet"
                df.to_parquet(temp_file_path)
                background_tasks.add_task(bronze_service.process_bronze_data, temp_file_path)
                return {"message": f"Added {len(df)} new record(s) to bronze and ETL processing started"}
            except ValueError as e:
                logger.error(f"Invalid data: {str(e)}")
                raise HTTPException(status_code=400, detail=str(e))
            except Exception as e:
                logger.error(f"Failed to create bronze data: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.put("/update-full-etl/", response_model=Dict[str, str])
        async def update_full_etl(
            updates: Union[Dict[str, Any], List[Dict[str, Any]]],
            background_tasks: BackgroundTasks,
            etl_service: ETLService = Depends(self.get_etl_service)
        ):
            try:
                background_tasks.add_task(etl_service.update_and_run_full_etl, updates)
                update_count = len([updates]) if isinstance(updates, dict) else len(updates)
                return {"message": f"Updating {update_count} record(s), full ETL with truncate-and-load started"}
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            except Exception as e:
                logger.error(f"Failed to process update and ETL: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.delete("/delete-full-etl/", response_model=Dict[str, str])
        async def delete_full_etl(
            bronze_ids: Union[int, List[int]],  # Accept single int or list of ints
            background_tasks: BackgroundTasks,
            etl_service: ETLService = Depends(self.get_etl_service)
        ):
            try:
                background_tasks.add_task(etl_service.delete_and_run_full_etl, bronze_ids)
                id_count = 1 if isinstance(bronze_ids, int) else len(bronze_ids)
                return {"message": f"Deletion of {id_count} record(s) started, full ETL with truncate-and-load triggered"}
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
            except Exception as e:
                logger.error(f"Failed to process delete and ETL: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))
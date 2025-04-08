# movies_data_pipeline/controllers/seed_controller.py
from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends
from pathlib import Path
import shutil
import logging
from movies_data_pipeline.services.bronze_service import BronzeService
from movies_data_pipeline.services.etl_service import ETLService
from movies_data_pipeline.services.auth_service import get_current_user
import os
from datetime import datetime

logger = logging.getLogger(__name__)

class SeedController:
    def __init__(self):
        """Initialize the SeedController with a router."""
        self.router = APIRouter()
        self.bronze_base_path = Path(os.getenv("BRONZE_BASE_PATH"))
        if not self.bronze_base_path:
            raise ValueError("BRONZE_BASE_PATH environment variable not set")
        self._register_routes()

    def get_etl_service(self) -> ETLService:
        """Provide an ETLService instance."""
        return ETLService()

    def get_bronze_service(self, etl_service: ETLService = Depends(lambda: ETLService())) -> BronzeService:
        """Provide a BronzeService instance with its dependencies."""
        bronze_file_path = self.bronze_base_path / "bronze_movies.parquet"
        return BronzeService(str(bronze_file_path), etl_service)

    def _register_routes(self):
        @self.router.post("/")
        async def seed_data(
            file: UploadFile = File(...),
            background_tasks: BackgroundTasks = None,
            bronze_service: BronzeService = Depends(self.get_bronze_service),
            current_user: str = Depends(get_current_user)
        ):
            """Seed data by saving the uploaded file to bronze with a timestamp and processing it in the background."""
            # Validate file type
            file_type = file.filename.split(".")[-1].lower()
            if file_type not in ["csv", "json"]:
                raise HTTPException(status_code=400, detail="Unsupported file type. Use 'csv', 'json'")

            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            original_name = Path(file.filename).stem
            extension = Path(file.filename).suffix
            timestamped_filename = f"{original_name}_{timestamp}{extension}"
            raw_file_path = self.bronze_base_path / timestamped_filename

            try:
                # Save the uploaded file with timestamp to bronze directory
                with raw_file_path.open("wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)
                logger.info(f"Raw file saved to bronze layer at {raw_file_path}")

                # Schedule processing in the background
                logger.info(f"Scheduling bronze processing for {timestamped_filename}")
                background_tasks.add_task(bronze_service.process_bronze_data, str(raw_file_path))
                return {"message": f"Data seeding started for {timestamped_filename}, processing in the background"}
            except Exception as e:
                logger.error(f"Failed to save file {file.filename} to bronze: {str(e)}")
                raise HTTPException(status_code=500, detail=f"Failed to initiate seeding: {str(e)}")
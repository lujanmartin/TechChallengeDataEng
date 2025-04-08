# movies_data_pipeline/api/routes/seed.py
from fastapi import APIRouter
from movies_data_pipeline.controllers.seed_controller import SeedController

seed_controller = SeedController()
router = seed_controller.router

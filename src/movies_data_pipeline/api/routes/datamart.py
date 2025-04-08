from fastapi import APIRouter
from movies_data_pipeline.controllers.datamart_controller import DataMartController

router = APIRouter(prefix="/datamart", tags=["datamart"])
datamart_controller = DataMartController()
router.include_router(datamart_controller.router)
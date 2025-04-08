from fastapi import APIRouter, Depends
from movies_data_pipeline.controllers.datamart_controller import DataMartController
from movies_data_pipeline.services.auth_service import get_current_user
from typing import List, Dict, Any

router = APIRouter(prefix="/datamart", tags=["datamart"])
datamart_controller = DataMartController()

@router.get("/revenue_by_genre_year", response_model=List[Dict[str, Any]], dependencies=[Depends(get_current_user)])
async def get_revenue_by_genre_year():
    """Get revenue aggregated by genre and year."""
    return datamart_controller.get_revenue_by_genre_year()

@router.get("/top_movies_by_revenue", response_model=List[Dict[str, Any]], dependencies=[Depends(get_current_user)])
async def get_top_movies_by_revenue():
    """Get top 10 movies by revenue."""
    return datamart_controller.get_top_movies_by_revenue()
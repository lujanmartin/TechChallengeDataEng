# movies_data_pipeline/controllers/datamart_controller.py
from fastapi import APIRouter, Depends, HTTPException
from movies_data_pipeline.data_access.database import get_session_direct
from movies_data_pipeline.services.auth_service import get_current_user
from sqlmodel import text
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class DataMartController:
    def __init__(self):
        self.router = APIRouter()
        self._register_routes()

    def _register_routes(self):
        @self.router.get(
            "/revenue_by_genre_year",
            response_model=List[Dict[str, Any]],
            dependencies=[Depends(get_current_user)],
        )
        async def get_revenue_by_genre_year():
            """Get revenue aggregated by genre and year."""
            try:
                with get_session_direct() as session:
                    result = (
                        session.exec(
                            text(
                                "SELECT genre_name, year, total_revenue, movie_count, lineage_id FROM dm_revenue_by_genre_year"
                            )
                        )
                        .mappings()
                        .all()
                    )
                    if not result:
                        logger.warning("No data found in dm_revenue_by_genre_year")
                        return []
                    return [dict(row) for row in result]
            except Exception as e:
                logger.error(f"Failed to fetch revenue by genre and year: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

        @self.router.get(
            "/top_movies_by_revenue",
            response_model=List[Dict[str, Any]],
            dependencies=[Depends(get_current_user)],
        )
        async def get_top_movies_by_revenue():
            """Get top 10 movies by revenue."""
            try:
                with get_session_direct() as session:
                    result = (
                        session.exec(
                            text(
                                "SELECT movie_id, title, revenue, release_year, rank, lineage_id FROM dm_top_movies_by_revenue ORDER BY rank"
                            )
                        )
                        .mappings()
                        .all()
                    )
                    if not result:
                        logger.warning("No data found in dm_top_movies_by_revenue")
                        return []
                    return [dict(row) for row in result]
            except Exception as e:
                logger.error(f"Failed to fetch top movies by revenue: {str(e)}")
                raise HTTPException(status_code=500, detail=str(e))

from fastapi import HTTPException
from movies_data_pipeline.data_access.database import get_session_direct
from sqlmodel import text
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataMartController:
    def get_revenue_by_genre_year(self) -> List[Dict[str, Any]]:
        """Fetch revenue by genre and year from the Data Mart materialized view."""
        try:
            with get_session_direct() as session:
                result = session.exec(
                    text("SELECT genre_name, year, total_revenue, movie_count, lineage_id FROM dm_revenue_by_genre_year")
                ).mappings().all()
                if not result:
                    logger.warning("No data found in dm_revenue_by_genre_year")
                    return []
                return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Failed to fetch revenue by genre and year: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    def get_top_movies_by_revenue(self) -> List[Dict[str, Any]]:
        """Fetch top 10 movies by revenue from the Data Mart materialized view."""
        try:
            with get_session_direct() as session:
                result = session.exec(
                    text("SELECT movie_id, title, revenue, release_year, rank, lineage_id FROM dm_top_movies_by_revenue ORDER BY rank")
                ).mappings().all()
                if not result:
                    logger.warning("No data found in dm_top_movies_by_revenue")
                    return []
                return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Failed to fetch top movies by revenue: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))
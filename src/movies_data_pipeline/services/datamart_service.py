from sqlmodel import Session
from sqlalchemy.sql import text 
from movies_data_pipeline.data_access.database import get_session_direct
import logging

logger = logging.getLogger(__name__)

class DataMartService:
    def refresh_datamart(self):
        """Refresh Data Mart materialized views."""
        with get_session_direct() as session:
            try:
                logger.info("Refreshing Data Mart materialized views")
                session.exec(text("REFRESH MATERIALIZED VIEW dm_revenue_by_genre_year;"))
                session.exec(text("REFRESH MATERIALIZED VIEW dm_top_movies_by_revenue;"))
                session.commit()
                logger.info("Data Mart materialized views refreshed successfully")
            except Exception as e:
                session.rollback()
                logger.error(f"Failed to refresh Data Mart: {str(e)}")
                raise
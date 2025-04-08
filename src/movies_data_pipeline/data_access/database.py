import os
from sqlmodel import create_engine, SQLModel, Session
from sqlalchemy.sql import text
import logging
from typing import Generator

from movies_data_pipeline.data_access.models.gold import (
    DimMovie,
    DimDate,
    DimCountry,
    DimLanguage,
    DimCrew,
    DimGenre,
    BridgeMovieGenre,
    BridgeMovieCrew,
    FactMovieMetrics,
    LineageLog,
)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set")

# Configure logging
logger = logging.getLogger(__name__)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

# Create engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_size=5,
    max_overflow=12,
)


def init_db():
    """Initialize the database by creating all gold layer tables."""
    try:
        logger.info("Initializing database tables")
        SQLModel.metadata.create_all(engine)
        # Create Data Mart materialized views
        with Session(engine) as session:
            # Drop existing materialized views if they exist (for idempotency)
            session.exec(
                text(
                    "DROP MATERIALIZED VIEW IF EXISTS dm_revenue_by_genre_year CASCADE;"
                )
            )
            session.exec(
                text(
                    "DROP MATERIALIZED VIEW IF EXISTS dm_top_movies_by_revenue CASCADE;"
                )
            )

            # Create dm_revenue_by_genre_year materialized view
            session.exec(
                text(
                    """
                CREATE MATERIALIZED VIEW dm_revenue_by_genre_year AS
                SELECT 
                    dg.genre_name,
                    dd.year,
                    SUM(fmm.revenue) AS total_revenue,
                    COUNT(DISTINCT fmm.movie_id) AS movie_count,
                    'datamart-' || CURRENT_TIMESTAMP AS lineage_id
                FROM fact_movie_metrics fmm
                JOIN bridge_movie_genre bmg ON fmm.movie_id = bmg.movie_id
                JOIN dim_genre dg ON bmg.genre_id = dg.genre_id
                JOIN dim_date dd ON fmm.date_id = dd.date_id
                GROUP BY dg.genre_name, dd.year;
            """
                )
            )
            session.exec(
                text(
                    "CREATE UNIQUE INDEX dm_revenue_by_genre_year_idx ON dm_revenue_by_genre_year (genre_name, year);"
                )
            )

            # Create dm_top_movies_by_revenue materialized view
            session.exec(
                text(
                    """
                CREATE MATERIALIZED VIEW dm_top_movies_by_revenue AS
                SELECT 
                    fmm.movie_id,
                    dm.name AS title,
                    fmm.revenue,
                    dd.year AS release_year,
                    ROW_NUMBER() OVER (ORDER BY fmm.revenue DESC) AS rank,
                    'datamart-' || CURRENT_TIMESTAMP AS lineage_id
                FROM fact_movie_metrics fmm
                JOIN dim_movie dm ON fmm.movie_id = dm.movie_id
                JOIN dim_date dd ON fmm.date_id = dd.date_id
                WHERE fmm.revenue IS NOT NULL
                ORDER BY fmm.revenue DESC
                LIMIT 10;
            """
                )
            )
            session.exec(
                text(
                    "CREATE UNIQUE INDEX dm_top_movies_by_revenue_idx ON dm_top_movies_by_revenue (movie_id);"
                )
            )

            session.commit()

        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")
        raise


def get_session() -> Generator[Session, None, None]:
    """Provide a database session for dependency injection."""
    with Session(engine) as session:
        yield session


def get_session_direct() -> Session:
    """Provide a direct database session for non-dependency use."""
    return Session(engine)


def get_db_engine():
    """Provide the database engine for dependency injection."""
    return engine

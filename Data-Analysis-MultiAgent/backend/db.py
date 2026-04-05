import sqlite3
import os
from datetime import datetime
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# Database file location
DB_DIR = Path("data")
DB_DIR.mkdir(exist_ok=True)
DB_FILE = DB_DIR / "analysis.db"


def get_db_connection():
    """Get a connection to the SQLite database."""
    conn = sqlite3.connect(str(DB_FILE))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Initialize the database with required tables."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create analysis history table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analysis_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_hash TEXT UNIQUE NOT NULL,
            raw_data TEXT,
            clean_data TEXT,
            stats_summary TEXT,
            charts TEXT,
            insights TEXT,
            errors TEXT,
            completed_agents TEXT,
            analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """)

    # Create analysis metadata table for faster lookups
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analysis_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_size INTEGER,
            row_count INTEGER,
            column_count INTEGER,
            completeness REAL,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (analysis_id) REFERENCES analysis_history(id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """)

    # Create indices for faster queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_user_id ON analysis_history(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_file_hash ON analysis_history(file_hash)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metadata_user_id ON analysis_metadata(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_metadata_file_name ON analysis_metadata(file_name)")

    conn.commit()
    conn.close()
    logger.info("Database initialized successfully")


def close_db_connection(conn):
    """Close database connection."""
    if conn:
        conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_FILE}")

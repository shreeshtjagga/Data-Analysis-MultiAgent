import hashlib
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from db import get_db_connection, close_db_connection
import pandas as pd
import io

logger = logging.getLogger(__name__)


def compute_file_hash(file_bytes: bytes) -> str:
    """Compute SHA256 hash of file content."""
    hash_obj = hashlib.sha256(file_bytes)
    return hash_obj.hexdigest()


def save_analysis(
    user_id: int,
    file_name: str,
    file_hash: str,
    file_size: int,
    analysis_result: dict,
    file_bytes: bytes
) -> dict:
    """
    Save an analysis result to the database.
    
    Args:
        user_id: ID of the user
        file_name: Name of the uploaded file
        file_hash: Hash of file content
        file_size: Size of the file in bytes
        analysis_result: Complete analysis result dictionary
        file_bytes: Raw file bytes for storage
    
    Returns:
        dict with success status and message
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Extract stats
        stats = analysis_result.get("stats_summary", {})
        row_count = stats.get("row_count")
        column_count = stats.get("column_count")
        completeness = stats.get("data_quality", {}).get("completeness")

        # Serialize data as JSON
        raw_data = analysis_result.get("raw_df")
        clean_data = analysis_result.get("clean_df")
        
        # Convert DataFrame to JSON-serializable dict if needed
        if isinstance(raw_data, pd.DataFrame):
            raw_data = raw_data.head(100).to_dict(orient="records")
        if isinstance(clean_data, pd.DataFrame):
            clean_data = clean_data.head(100).to_dict(orient="records")

        raw_data_json = json.dumps(raw_data) if raw_data else None
        clean_data_json = json.dumps(clean_data) if clean_data else None
        stats_json = json.dumps(stats) if stats else None
        charts_json = json.dumps({}) if not analysis_result.get("charts") else "{}"  # Charts are plotly objects, store empty
        insights_json = json.dumps(analysis_result.get("insights", {})) if analysis_result.get("insights") else None
        errors_json = json.dumps(analysis_result.get("errors", [])) if analysis_result.get("errors") else None
        agents_json = json.dumps(analysis_result.get("completed_agents", [])) if analysis_result.get("completed_agents") else None

        # Check if this file hash already exists for this user
        cursor.execute(
            "SELECT id FROM analysis_history WHERE user_id = ? AND file_hash = ?",
            (user_id, file_hash)
        )
        existing = cursor.fetchone()

        if existing:
            # Update existing analysis
            analysis_id = existing["id"]
            cursor.execute(
                """UPDATE analysis_history 
                   SET raw_data = ?, clean_data = ?, stats_summary = ?, 
                       insights = ?, errors = ?, completed_agents = ?,
                       analysis_date = CURRENT_TIMESTAMP
                   WHERE id = ?""",
                (raw_data_json, clean_data_json, stats_json, 
                 insights_json, errors_json, agents_json, analysis_id)
            )
            logger.info(f"Analysis updated for user {user_id}: {file_name}")
            message = "Analysis updated (same file detected)"
        else:
            # Insert new analysis
            cursor.execute(
                """INSERT INTO analysis_history 
                   (user_id, file_name, file_hash, raw_data, clean_data, 
                    stats_summary, charts, insights, errors, completed_agents)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (user_id, file_name, file_hash, raw_data_json, clean_data_json,
                 stats_json, charts_json, insights_json, errors_json, agents_json)
            )
            analysis_id = cursor.lastrowid
            logger.info(f"Analysis saved for user {user_id}: {file_name}")
            message = "Analysis saved successfully"

        # Update/insert metadata for quick lookups
        cursor.execute(
            "SELECT id FROM analysis_metadata WHERE analysis_id = ?",
            (analysis_id,)
        )
        meta_exists = cursor.fetchone()

        if meta_exists:
            cursor.execute(
                """UPDATE analysis_metadata 
                   SET file_size = ?, row_count = ?, column_count = ?, 
                       completeness = ?, analyzed_at = CURRENT_TIMESTAMP
                   WHERE analysis_id = ?""",
                (file_size, row_count, column_count, completeness, analysis_id)
            )
        else:
            cursor.execute(
                """INSERT INTO analysis_metadata 
                   (analysis_id, user_id, file_name, file_size, row_count, 
                    column_count, completeness)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (analysis_id, user_id, file_name, file_size, row_count, 
                 column_count, completeness)
            )

        conn.commit()
        close_db_connection(conn)

        return {
            "success": True,
            "message": message,
            "analysis_id": analysis_id
        }

    except Exception as e:
        logger.error(f"Save analysis error: {e}")
        return {
            "success": False,
            "message": f"Failed to save analysis: {str(e)}"
        }


def get_analysis_by_hash(user_id: int, file_hash: str) -> Optional[dict]:
    """
    Retrieve a previously saved analysis by file hash.
    
    Args:
        user_id: ID of the user
        file_hash: Hash of the file content
    
    Returns:
        Analysis data if found, None otherwise
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """SELECT id, file_name, stats_summary, insights, errors, 
                      completed_agents, analysis_date
               FROM analysis_history 
               WHERE user_id = ? AND file_hash = ?""",
            (user_id, file_hash)
        )
        result = cursor.fetchone()
        close_db_connection(conn)

        if not result:
            return None

        # Parse JSON fields
        analysis = {
            "id": result["id"],
            "file_name": result["file_name"],
            "file_hash": file_hash,
            "stats_summary": json.loads(result["stats_summary"]) if result["stats_summary"] else {},
            "insights": json.loads(result["insights"]) if result["insights"] else {},
            "errors": json.loads(result["errors"]) if result["errors"] else [],
            "completed_agents": json.loads(result["completed_agents"]) if result["completed_agents"] else [],
            "analysis_date": result["analysis_date"],
            "from_cache": True
        }

        logger.info(f"Retrieved cached analysis for user {user_id}: {result['file_name']}")
        return analysis

    except Exception as e:
        logger.error(f"Get analysis by hash error: {e}")
        return None


def get_user_analysis_history(user_id: int, limit: int = 50) -> List[dict]:
    """
    Get all analysis history for a user.
    
    Args:
        user_id: ID of the user
        limit: Maximum number of records to return
    
    Returns:
        List of analysis history records
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """SELECT m.id, m.file_name, h.file_hash, m.row_count, 
                      m.column_count, m.completeness, m.analyzed_at
               FROM analysis_metadata m
               JOIN analysis_history h ON m.analysis_id = h.id
               WHERE m.user_id = ?
               ORDER BY m.analyzed_at DESC
               LIMIT ?""",
            (user_id, limit)
        )
        results = cursor.fetchall()
        close_db_connection(conn)

        history = []
        for row in results:
            history.append({
                "id": row["id"],
                "file_name": row["file_name"],
                "file_hash": row["file_hash"],
                "row_count": row["row_count"],
                "column_count": row["column_count"],
                "completeness": row["completeness"],
                "analyzed_at": row["analyzed_at"]
            })

        logger.info(f"Retrieved {len(history)} analysis records for user {user_id}")
        return history

    except Exception as e:
        logger.error(f"Get analysis history error: {e}")
        return []


def delete_analysis(user_id: int, analysis_id: int) -> dict:
    """
    Delete an analysis record.
    
    Args:
        user_id: ID of the user
        analysis_id: ID of the analysis to delete
    
    Returns:
        dict with success status and message
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verify ownership
        cursor.execute(
            "SELECT user_id FROM analysis_history WHERE id = ?",
            (analysis_id,)
        )
        result = cursor.fetchone()

        if not result or result["user_id"] != user_id:
            close_db_connection(conn)
            return {
                "success": False,
                "message": "Analysis not found or access denied"
            }

        # Delete from both tables (cascade will handle it)
        cursor.execute("DELETE FROM analysis_history WHERE id = ?", (analysis_id,))
        cursor.execute("DELETE FROM analysis_metadata WHERE analysis_id = ?", (analysis_id,))

        conn.commit()
        close_db_connection(conn)

        logger.info(f"Analysis {analysis_id} deleted for user {user_id}")
        return {
            "success": True,
            "message": "Analysis deleted successfully"
        }

    except Exception as e:
        logger.error(f"Delete analysis error: {e}")
        return {
            "success": False,
            "message": f"Failed to delete analysis: {str(e)}"
        }


def get_analysis_summary_stats(analysis_id: int) -> Optional[dict]:
    """Get summary statistics for an analysis."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT stats_summary FROM analysis_history WHERE id = ?",
            (analysis_id,)
        )
        result = cursor.fetchone()
        close_db_connection(conn)

        if not result or not result["stats_summary"]:
            return None

        stats = json.loads(result["stats_summary"])
        return {
            "row_count": stats.get("row_count", 0),
            "column_count": stats.get("column_count", 0),
            "missing_cells": stats.get("data_quality", {}).get("missing_cells", 0),
            "duplicate_rows": stats.get("data_quality", {}).get("duplicate_rows", 0),
            "completeness": stats.get("data_quality", {}).get("completeness", 100),
            "outlier_cols": len(stats.get("outliers", {})),
            "strong_correlations": len(stats.get("strong_correlations", []))
        }

    except Exception as e:
        logger.error(f"Get analysis summary stats error: {e}")
        return None

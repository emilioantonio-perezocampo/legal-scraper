"""
BJV Session Manager for checkpoint persistence.
"""
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List


class BJVSessionManager:
    """Manages scraping sessions and checkpoints."""

    def __init__(self, output_dir: str = "bjv_data"):
        """
        Initialize session manager.

        Args:
            output_dir: Directory for storing checkpoints
        """
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def save_checkpoint(
        self,
        session_id: str,
        state: str,
        stats: Dict[str, int],
        pending_ids: tuple,
        processed_ids: tuple,
    ) -> Path:
        """
        Save checkpoint to disk.

        Args:
            session_id: Session identifier
            state: Current pipeline state
            stats: Statistics dictionary
            pending_ids: IDs still pending processing
            processed_ids: IDs already processed

        Returns:
            Path to saved checkpoint file
        """
        checkpoint = {
            "session_id": session_id,
            "state": state,
            "stats": stats,
            "pending_ids": list(pending_ids),
            "processed_ids": list(processed_ids),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        filepath = self._output_dir / f"checkpoint_{session_id}.json"

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2, ensure_ascii=False)

        return filepath

    def load_checkpoint(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Load checkpoint from disk.

        Args:
            session_id: Session identifier

        Returns:
            Checkpoint data or None if not found
        """
        filepath = self._output_dir / f"checkpoint_{session_id}.json"

        if not filepath.exists():
            return None

        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)

    def list_sessions(self) -> List[str]:
        """
        List all available sessions.

        Returns:
            List of session IDs, sorted descending
        """
        checkpoints = self._output_dir.glob("checkpoint_*.json")
        sessions = []

        for cp in checkpoints:
            session_id = cp.stem.replace("checkpoint_", "")
            sessions.append(session_id)

        return sorted(sessions, reverse=True)

    def get_latest_session(self) -> Optional[str]:
        """
        Get the most recent session ID.

        Returns:
            Latest session ID or None if no sessions
        """
        sessions = self.list_sessions()
        return sessions[0] if sessions else None

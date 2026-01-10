"""
Checkpoint Actor

Persists scraping progress for resume capability.
Handles checkpoint save/load/delete operations.
"""
import json
import asyncio
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

from .base import BaseActor
from .messages import (
    GuardarCheckpoint,
    CargarCheckpoint,
    CheckpointGuardado,
)
from src.domain.scjn_entities import ScrapingCheckpoint


class CheckpointActor(BaseActor):
    """
    Actor that persists scraping progress for resume capability.

    Handles:
    - GuardarCheckpoint: Save checkpoint to file
    - CargarCheckpoint: Load checkpoint by session_id
    - ("LIST",): List all checkpoint session IDs
    - ("DELETE", session_id): Delete a checkpoint
    """

    def __init__(self, checkpoint_dir: str = "checkpoints"):
        """
        Initialize the checkpoint actor.

        Args:
            checkpoint_dir: Directory to store checkpoint files
        """
        super().__init__()
        self._checkpoint_dir = Path(checkpoint_dir)
        self._checkpoints: Dict[str, ScrapingCheckpoint] = {}
        self._lock = asyncio.Lock()

    async def start(self):
        """Start the actor and load existing checkpoints."""
        await super().start()
        self._checkpoint_dir.mkdir(parents=True, exist_ok=True)
        await self._load_existing_checkpoints()

    async def handle_message(self, message):
        """Handle incoming messages."""
        if isinstance(message, GuardarCheckpoint):
            return await self._save_checkpoint(message.checkpoint, message.correlation_id)

        elif isinstance(message, CargarCheckpoint):
            return self._checkpoints.get(message.session_id)

        elif isinstance(message, tuple) and len(message) >= 1:
            if message[0] == "LIST":
                return tuple(self._checkpoints.keys())
            elif message[0] == "DELETE" and len(message) >= 2:
                await self._delete_checkpoint(message[1])
                return None

        return None

    async def _load_existing_checkpoints(self) -> None:
        """Load all existing checkpoint files from directory."""
        if not self._checkpoint_dir.exists():
            return

        for checkpoint_file in self._checkpoint_dir.glob("*.json"):
            try:
                checkpoint = await self._load_checkpoint_file(checkpoint_file)
                if checkpoint:
                    self._checkpoints[checkpoint.session_id] = checkpoint
            except Exception:
                # Skip corrupted files
                continue

    async def _load_checkpoint_file(self, file_path: Path) -> Optional[ScrapingCheckpoint]:
        """Load a single checkpoint file."""
        try:
            loop = asyncio.get_event_loop()
            content = await loop.run_in_executor(None, file_path.read_text)
            data = json.loads(content)

            # Validate required fields
            if "session_id" not in data:
                return None

            return ScrapingCheckpoint(
                session_id=data["session_id"],
                last_processed_q_param=data.get("last_processed_q_param", ""),
                processed_count=data.get("processed_count", 0),
                failed_q_params=tuple(data.get("failed_q_params", [])),
                created_at=datetime.fromisoformat(data["created_at"])
                if "created_at" in data
                else datetime.now(),
            )
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    async def _save_checkpoint(
        self, checkpoint: ScrapingCheckpoint, correlation_id: str
    ) -> CheckpointGuardado:
        """Save checkpoint to file and memory."""
        async with self._lock:
            # Store in memory
            self._checkpoints[checkpoint.session_id] = checkpoint

            # Serialize to file
            data = {
                "session_id": checkpoint.session_id,
                "last_processed_q_param": checkpoint.last_processed_q_param,
                "processed_count": checkpoint.processed_count,
                "failed_q_params": list(checkpoint.failed_q_params),
                "created_at": checkpoint.created_at.isoformat(),
            }

            file_path = self._checkpoint_dir / f"{checkpoint.session_id}.json"
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, lambda: file_path.write_text(json.dumps(data, indent=2))
            )

            return CheckpointGuardado(
                correlation_id=correlation_id,
                session_id=checkpoint.session_id,
                processed_count=checkpoint.processed_count,
            )

    async def _delete_checkpoint(self, session_id: str) -> None:
        """Delete checkpoint from file and memory."""
        async with self._lock:
            # Remove from memory
            self._checkpoints.pop(session_id, None)

            # Remove file
            file_path = self._checkpoint_dir / f"{session_id}.json"
            if file_path.exists():
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, file_path.unlink)

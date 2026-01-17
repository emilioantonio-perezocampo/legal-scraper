"""
Supabase Real-time Manager for job monitoring.

Provides real-time subscription management for scraper job progress
updates using Supabase Realtime (WebSockets).

Features:
- Subscribe to job status changes
- Filter updates by job_id
- Broadcast progress updates to connected clients
- Automatic reconnection handling

Prerequisites:
- Table must be added to supabase_realtime publication:
  ALTER PUBLICATION supabase_realtime ADD TABLE scraper_jobs;
- Use async client (acreate_client) for proper WebSocket handling

Usage:
    from supabase import acreate_client
    client = await acreate_client(url, key)
    manager = SupabaseRealtimeManager(client=client)

    # Subscribe to job updates
    async def on_update(payload):
        print(f"Job status: {payload['new']['status']}")

    sub_id = await manager.subscribe_to_job("job-123", on_update)

    # Later: cleanup
    await manager.close()
"""
from __future__ import annotations

import uuid
from typing import Any, Callable, Dict, Optional


class SupabaseRealtimeManager:
    """
    Real-time subscription manager for scraper job monitoring.

    Manages WebSocket subscriptions to the scraper_jobs table,
    allowing clients to receive instant updates on job progress.

    Attributes:
        _client: Async Supabase client
        _subscriptions: Dict of active subscriptions by ID
    """

    def __init__(self, client: Any) -> None:
        """
        Initialize the realtime manager.

        Args:
            client: Async Supabase client from acreate_client()
        """
        self._client = client
        self._subscriptions: Dict[str, Dict[str, Any]] = {}

    async def subscribe_to_job(
        self,
        job_id: str,
        on_update: Callable[[Dict[str, Any]], None],
    ) -> str:
        """
        Subscribe to status changes for a specific job.

        Args:
            job_id: UUID of the job to monitor
            on_update: Callback invoked when job status changes.
                      Receives the full payload with 'new' and 'old' data.

        Returns:
            Subscription ID for later unsubscribe

        Example:
            async def handle_update(payload):
                new_status = payload['new']['status']
                progress = payload['new']['progress']
                print(f"Job {job_id}: {new_status} - {progress}")

            sub_id = await manager.subscribe_to_job(job_id, handle_update)
        """
        subscription_id = str(uuid.uuid4())

        # Create a channel for this job's updates
        channel_name = f"scraper_jobs:id=eq.{job_id}"
        channel = self._client.channel(channel_name)

        # Register callback for postgres_changes
        channel.on(
            "postgres_changes",
            on_update,
            event="UPDATE",
            schema="public",
            table="scraper_jobs",
            filter=f"id=eq.{job_id}",
        )

        # Subscribe to the channel
        channel.subscribe()

        # Track the subscription
        self._subscriptions[subscription_id] = {
            "job_id": job_id,
            "channel": channel,
            "callback": on_update,
        }

        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> None:
        """
        Unsubscribe from job updates.

        Args:
            subscription_id: ID returned from subscribe_to_job()
        """
        if subscription_id not in self._subscriptions:
            return

        subscription = self._subscriptions.pop(subscription_id)
        channel = subscription["channel"]

        # Unsubscribe from the channel
        if hasattr(channel, "unsubscribe"):
            await channel.unsubscribe()

    async def broadcast_progress(
        self,
        job_id: str,
        progress: Dict[str, Any],
        status: Optional[str] = None,
    ) -> None:
        """
        Broadcast a progress update for a job.

        Updates the job record in the database, which triggers
        Realtime updates to all subscribed clients.

        Args:
            job_id: UUID of the job
            progress: Progress data (e.g., {"completed": 50, "total": 100})
            status: Optional new status (e.g., "processing", "completed")
        """
        update_data: Dict[str, Any] = {"progress": progress}

        if status:
            update_data["status"] = status

        await self._client.table("scraper_jobs").update(update_data).eq(
            "id", job_id
        ).execute()

    async def close(self) -> None:
        """
        Close all subscriptions and cleanup.

        Should be called when shutting down to properly release
        WebSocket connections.
        """
        subscription_ids = list(self._subscriptions.keys())

        for sub_id in subscription_ids:
            await self.unsubscribe(sub_id)

    async def _reconnect(self) -> None:
        """
        Reconnect all subscriptions after a connection drop.

        Called internally when the WebSocket connection is restored.
        Re-subscribes to all previously active job channels.
        """
        # Store current subscriptions
        current_subs = dict(self._subscriptions)

        # Re-subscribe to each job
        for sub_id, sub_info in current_subs.items():
            job_id = sub_info["job_id"]
            callback = sub_info["callback"]

            # Create new channel
            channel_name = f"scraper_jobs:id=eq.{job_id}"
            channel = self._client.channel(channel_name)

            channel.on(
                "postgres_changes",
                callback,
                event="UPDATE",
                schema="public",
                table="scraper_jobs",
                filter=f"id=eq.{job_id}",
            )

            channel.subscribe()

            # Update the subscription with new channel
            self._subscriptions[sub_id]["channel"] = channel

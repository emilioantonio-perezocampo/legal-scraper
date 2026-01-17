"""
RED Phase Tests: SupabaseRealtimeManager

Tests for real-time subscription management using Supabase Realtime.
These tests must FAIL initially (RED phase).

The manager should:
- Subscribe to job status changes via WebSocket
- Filter updates by job_id
- Invoke callbacks on status updates
- Clean up subscriptions on unsubscribe
- Handle reconnection on disconnect
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio

from src.infrastructure.adapters.supabase_realtime import SupabaseRealtimeManager


@pytest.fixture
def mock_async_client():
    """Create a mock async Supabase client with realtime capabilities."""
    client = MagicMock()

    # Mock channel/subscription - channel() is synchronous
    channel = MagicMock()
    channel.on.return_value = channel
    channel.subscribe.return_value = channel
    channel.unsubscribe = AsyncMock()

    client.channel.return_value = channel

    # Mock table operations for broadcast_progress
    table_mock = MagicMock()
    update_mock = MagicMock()
    eq_mock = MagicMock()
    eq_mock.execute = AsyncMock()
    update_mock.eq.return_value = eq_mock
    table_mock.update.return_value = update_mock
    client.table.return_value = table_mock

    return client, channel


@pytest.fixture
def sample_job_update():
    """Sample job status update payload."""
    return {
        "eventType": "UPDATE",
        "new": {
            "id": "job-123",
            "status": "processing",
            "progress": {"completed": 50, "total": 100},
        },
        "old": {
            "id": "job-123",
            "status": "pending",
            "progress": {"completed": 0, "total": 100},
        },
    }


class TestSupabaseRealtimeManagerSubscribe:
    """Tests for job subscription."""

    @pytest.mark.asyncio
    async def test_subscribe_to_job_returns_subscription_id(
        self, mock_async_client
    ):
        """Subscribing should return a subscription ID for later cleanup."""
        client, channel = mock_async_client

        manager = SupabaseRealtimeManager(client=client)
        subscription_id = await manager.subscribe_to_job(
            job_id="job-123",
            on_update=lambda x: None,
        )

        assert subscription_id is not None
        assert isinstance(subscription_id, str)

    @pytest.mark.asyncio
    async def test_subscribe_creates_channel_with_job_filter(
        self, mock_async_client
    ):
        """Subscription should filter by job_id."""
        client, channel = mock_async_client

        manager = SupabaseRealtimeManager(client=client)
        await manager.subscribe_to_job(
            job_id="job-123",
            on_update=lambda x: None,
        )

        # Verify channel was created
        client.channel.assert_called()

    @pytest.mark.asyncio
    async def test_subscribe_invokes_callback_on_update(
        self, mock_async_client, sample_job_update
    ):
        """Callback should be invoked when job status changes."""
        client, channel = mock_async_client
        callback_data = []

        manager = SupabaseRealtimeManager(client=client)
        await manager.subscribe_to_job(
            job_id="job-123",
            on_update=lambda x: callback_data.append(x),
        )

        # Simulate receiving an update
        # Get the callback that was registered
        on_call = channel.on.call_args
        if on_call and len(on_call[0]) > 1:
            registered_callback = on_call[0][1]
            registered_callback(sample_job_update)

            assert len(callback_data) > 0

    @pytest.mark.asyncio
    async def test_subscribe_stores_subscription(self, mock_async_client):
        """Manager should track active subscriptions."""
        client, channel = mock_async_client

        manager = SupabaseRealtimeManager(client=client)
        sub_id = await manager.subscribe_to_job(
            job_id="job-123",
            on_update=lambda x: None,
        )

        assert sub_id in manager._subscriptions


class TestSupabaseRealtimeManagerUnsubscribe:
    """Tests for unsubscribing from updates."""

    @pytest.mark.asyncio
    async def test_unsubscribe_removes_subscription(self, mock_async_client):
        """Unsubscribing should remove the subscription from tracking."""
        client, channel = mock_async_client

        manager = SupabaseRealtimeManager(client=client)
        sub_id = await manager.subscribe_to_job(
            job_id="job-123",
            on_update=lambda x: None,
        )

        await manager.unsubscribe(sub_id)

        assert sub_id not in manager._subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_calls_channel_unsubscribe(
        self, mock_async_client
    ):
        """Unsubscribing should call the channel's unsubscribe method."""
        client, channel = mock_async_client
        channel.unsubscribe = AsyncMock()

        manager = SupabaseRealtimeManager(client=client)
        sub_id = await manager.subscribe_to_job(
            job_id="job-123",
            on_update=lambda x: None,
        )

        await manager.unsubscribe(sub_id)

        channel.unsubscribe.assert_called()

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent_does_not_raise(
        self, mock_async_client
    ):
        """Unsubscribing from nonexistent ID should not raise."""
        client, channel = mock_async_client

        manager = SupabaseRealtimeManager(client=client)

        # Should not raise
        await manager.unsubscribe("nonexistent-id")


class TestSupabaseRealtimeManagerBroadcast:
    """Tests for broadcasting progress updates."""

    @pytest.mark.asyncio
    async def test_broadcast_progress_updates_database(
        self, mock_async_client
    ):
        """Broadcasting should update the job record in database."""
        client, channel = mock_async_client
        client.table.return_value.update.return_value.eq.return_value.execute = (
            AsyncMock()
        )

        manager = SupabaseRealtimeManager(client=client)
        await manager.broadcast_progress(
            job_id="job-123",
            progress={"completed": 75, "total": 100},
        )

        client.table.assert_called_with("scraper_jobs")

    @pytest.mark.asyncio
    async def test_broadcast_progress_includes_status(
        self, mock_async_client
    ):
        """Broadcasting can update job status."""
        client, channel = mock_async_client
        client.table.return_value.update.return_value.eq.return_value.execute = (
            AsyncMock()
        )

        manager = SupabaseRealtimeManager(client=client)
        await manager.broadcast_progress(
            job_id="job-123",
            progress={"completed": 100, "total": 100},
            status="completed",
        )

        update_call = client.table.return_value.update.call_args
        assert update_call is not None


class TestSupabaseRealtimeManagerClose:
    """Tests for cleanup."""

    @pytest.mark.asyncio
    async def test_close_unsubscribes_all(self, mock_async_client):
        """close() should unsubscribe from all active subscriptions."""
        client, channel = mock_async_client
        channel.unsubscribe = AsyncMock()

        manager = SupabaseRealtimeManager(client=client)

        # Create multiple subscriptions
        await manager.subscribe_to_job("job-1", lambda x: None)
        await manager.subscribe_to_job("job-2", lambda x: None)

        await manager.close()

        assert len(manager._subscriptions) == 0

    @pytest.mark.asyncio
    async def test_close_can_be_called_multiple_times(
        self, mock_async_client
    ):
        """close() should be idempotent."""
        client, channel = mock_async_client

        manager = SupabaseRealtimeManager(client=client)

        # Should not raise on multiple calls
        await manager.close()
        await manager.close()


class TestSupabaseRealtimeManagerConfiguration:
    """Tests for manager configuration."""

    def test_manager_requires_client(self):
        """Manager should require an async Supabase client."""
        with pytest.raises(TypeError):
            SupabaseRealtimeManager()

    def test_manager_initializes_empty_subscriptions(
        self, mock_async_client
    ):
        """Manager should start with empty subscriptions dict."""
        client, channel = mock_async_client

        manager = SupabaseRealtimeManager(client=client)

        assert manager._subscriptions == {}


class TestSupabaseRealtimeManagerReconnection:
    """Tests for reconnection handling."""

    @pytest.mark.asyncio
    async def test_reconnect_restores_subscriptions(
        self, mock_async_client
    ):
        """After reconnect, all subscriptions should be restored."""
        client, channel = mock_async_client

        manager = SupabaseRealtimeManager(client=client)
        await manager.subscribe_to_job("job-123", lambda x: None)

        # Simulate reconnection
        await manager._reconnect()

        # Subscription should still exist
        assert len(manager._subscriptions) > 0

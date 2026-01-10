"""
Tests for BJV scraper actor.

Following RED-GREEN TDD: These tests define the expected behavior
for the BJV scraper actor that fetches book details.

Target: ~20 tests
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock


class TestBJVScraperActorInit:
    """Tests for scraper actor initialization."""

    def test_inherits_from_base_actor(self):
        """BJVScraperActor inherits from BJVBaseActor."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_base_actor import BJVBaseActor

        assert issubclass(BJVScraperActor, BJVBaseActor)

    def test_creates_with_coordinator(self):
        """BJVScraperActor requires coordinator."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        coordinator = Mock()
        rate_limiter = BJVRateLimiter()

        actor = BJVScraperActor(
            coordinator=coordinator,
            rate_limiter=rate_limiter,
        )

        assert actor._coordinator is coordinator

    def test_has_nombre(self):
        """BJVScraperActor has descriptive name."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        actor = BJVScraperActor(
            coordinator=Mock(),
            rate_limiter=BJVRateLimiter(),
        )

        assert "Scraper" in actor.nombre


class TestBJVScraperActorLifecycle:
    """Tests for scraper actor lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_session(self):
        """start() creates HTTP session."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        actor = BJVScraperActor(
            coordinator=Mock(),
            rate_limiter=BJVRateLimiter(),
        )

        await actor.start()

        assert actor._session is not None

        await actor.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_session(self):
        """stop() closes HTTP session."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        actor = BJVScraperActor(
            coordinator=Mock(),
            rate_limiter=BJVRateLimiter(),
        )

        await actor.start()
        session = actor._session
        await actor.stop()

        assert session.closed


class TestBJVScraperActorDownload:
    """Tests for book download handling."""

    @pytest.mark.asyncio
    async def test_handle_descargar_libro(self):
        """handle_message processes DescargarLibro."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.actors.bjv_messages import DescargarLibro

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVScraperActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        with patch.object(actor, '_fetch_libro_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = "<html><body><h1>Test Libro</h1></body></html>"

            await actor.start()
            msg = DescargarLibro(
                correlation_id="corr-123",
                libro_id="4567",
                url_detalle="https://biblio.juridicas.unam.mx/bjv/detalle-libro/4567",
            )
            await actor.handle_message(msg)
            await actor.stop()

        mock_fetch.assert_called()

    @pytest.mark.asyncio
    async def test_libro_parsed_correctly(self):
        """Downloaded libro HTML is parsed."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.actors.bjv_messages import DescargarLibro, LibroListo

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVScraperActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        html = """
        <html>
            <body>
                <h1>Derecho Civil Mexicano</h1>
                <p class="autor">Juan Pérez</p>
            </body>
        </html>
        """

        with patch.object(actor, '_fetch_libro_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = html

            await actor.start()
            msg = DescargarLibro(
                correlation_id="corr-123",
                libro_id="4567",
                url_detalle="https://biblio.juridicas.unam.mx/bjv/detalle-libro/4567",
            )
            await actor.handle_message(msg)
            await actor.stop()

        # Should have sent LibroListo or error
        coordinator.tell.assert_called()

    @pytest.mark.asyncio
    async def test_emits_libro_listo(self):
        """Actor emits LibroListo after successful parse."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.actors.bjv_messages import LibroListo

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVScraperActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        html = "<html><body><h1>Test Libro</h1></body></html>"

        with patch.object(actor, '_fetch_libro_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = html

            await actor.start()
            await actor._process_libro("corr-123", "4567", "https://test.com")
            await actor.stop()

        call_args = coordinator.tell.call_args[0][0]
        assert isinstance(call_args, LibroListo)


class TestBJVScraperActorRetry:
    """Tests for retry logic."""

    @pytest.mark.asyncio
    async def test_retry_on_network_error(self):
        """Network errors trigger retry."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        import aiohttp

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVScraperActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
            max_retries=2,
        )

        call_count = 0

        async def failing_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise aiohttp.ClientError("Connection failed")
            return "<html><body><h1>Success</h1></body></html>"

        with patch.object(actor, '_fetch_libro_page', side_effect=failing_fetch):
            await actor.start()
            await actor._process_libro("corr-123", "4567", "https://test.com")
            await actor.stop()

        # Should have retried
        assert call_count >= 1

    @pytest.mark.asyncio
    async def test_rate_limiter_used(self):
        """Rate limiter is used before fetching."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        rate_limiter = BJVRateLimiter()
        rate_limiter.acquire = AsyncMock()

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVScraperActor(
            coordinator=coordinator,
            rate_limiter=rate_limiter,
        )

        # Create a proper async context manager mock
        mock_response = MagicMock()
        mock_response.text = AsyncMock(return_value="<html></html>")
        mock_response.status = 200
        mock_response.raise_for_status = Mock()

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_response)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = Mock(return_value=mock_cm)
        actor._session = mock_session

        await actor._fetch_libro_page("https://example.com")

        rate_limiter.acquire.assert_called()


class TestBJVScraperActorErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_retry_exhausted_emits_error(self):
        """After retries exhausted, error is emitted."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter
        from src.infrastructure.actors.bjv_messages import ErrorProcesamiento
        import aiohttp

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVScraperActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
            max_retries=1,
        )

        async def always_fail(*args, **kwargs):
            raise aiohttp.ClientError("Connection failed")

        with patch.object(actor, '_fetch_libro_page', side_effect=always_fail):
            await actor.start()
            await actor._process_libro("corr-123", "4567", "https://test.com")
            await actor.stop()

        # Should have sent error
        call_args = coordinator.tell.call_args[0][0]
        assert isinstance(call_args, ErrorProcesamiento)

    @pytest.mark.asyncio
    async def test_forwards_to_coordinator(self):
        """Successful results are forwarded to coordinator."""
        from src.infrastructure.actors.bjv_scraper_actor import BJVScraperActor
        from src.infrastructure.actors.bjv_rate_limiter import BJVRateLimiter

        coordinator = Mock()
        coordinator.tell = AsyncMock()

        actor = BJVScraperActor(
            coordinator=coordinator,
            rate_limiter=BJVRateLimiter(),
        )

        html = "<html><body><h1>Test</h1></body></html>"

        with patch.object(actor, '_fetch_libro_page', new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = html

            await actor.start()
            await actor._process_libro("corr-123", "4567", "https://test.com")
            await actor.stop()

        coordinator.tell.assert_called()

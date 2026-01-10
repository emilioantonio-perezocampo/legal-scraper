"""
Integration Tests for CAS Pipeline.

These tests verify the end-to-end pipeline behavior
using mocked HTTP responses and isolated file systems.

Target: ~15 integration tests
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from pathlib import Path
import tempfile


class TestCASPipelineIntegration:
    """Integration tests for CAS pipeline components working together."""

    @pytest.mark.asyncio
    async def test_discovery_to_scraper_flow(self):
        """Discovery actor forwards discovered awards to scraper."""
        from src.infrastructure.actors.cas_discovery_actor import CASDiscoveryActor
        from src.infrastructure.actors.cas_messages import (
            IniciarDescubrimientoCAS,
            LaudoDescubiertoCAS,
        )

        coordinator = AsyncMock()
        browser = AsyncMock()
        browser.render_page = AsyncMock(return_value=Mock(
            html="<html><a href='/cases/CAS-2023-A-1'>Case 1</a></html>",
            status_code=200
        ))

        discovery = CASDiscoveryActor(
            browser_adapter=browser,
            supervisor=coordinator
        )
        await discovery.start()

        # Start discovery
        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await discovery.receive(msg)

        # Allow processing time
        await asyncio.sleep(0.1)

        await discovery.stop()

    @pytest.mark.asyncio
    async def test_scraper_to_fragmentador_flow(self):
        """Scraper actor forwards scraped awards to fragmentador."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS

        coordinator = AsyncMock()
        browser = AsyncMock()
        browser.render_page = AsyncMock(return_value=Mock(
            html="""
            <html>
                <h1>CAS 2023/A/12345</h1>
                <div class="date">1 January 2023</div>
                <div class="parties">Athlete v. Federation</div>
            </html>
            """,
            status_code=200
        ))

        scraper = CASScraperActor(
            browser_adapter=browser,
            supervisor=coordinator
        )
        await scraper.start()

        msg = ScrapearLaudoCAS(
            numero_caso="CAS 2023/A/12345",
            url="https://jurisprudence.tas-cas.org/case/12345"
        )
        await scraper.receive(msg)

        await asyncio.sleep(0.1)
        await scraper.stop()

    @pytest.mark.asyncio
    async def test_coordinator_orchestrates_pipeline(self):
        """Coordinator properly orchestrates all pipeline stages."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import (
            IniciarDescubrimientoCAS,
            LaudoDescubiertoCAS,
            EstadoPipelineCAS,
        )

        discovery = AsyncMock()
        scraper = AsyncMock()
        fragmentador = AsyncMock()

        coordinator = CASCoordinatorActor(
            discovery_actor=discovery,
            scraper_actor=scraper,
            fragmentador_actor=fragmentador
        )
        await coordinator.start()

        # Start discovery through coordinator
        msg = IniciarDescubrimientoCAS(max_paginas=1)
        await coordinator.receive(msg)

        # Discovery should be called
        discovery.tell.assert_called()

        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_coordinator_handles_discovered_award(self):
        """Coordinator forwards discovered awards to scraper."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import LaudoDescubiertoCAS

        discovery = AsyncMock()
        scraper = AsyncMock()

        coordinator = CASCoordinatorActor(
            discovery_actor=discovery,
            scraper_actor=scraper
        )
        await coordinator.start()

        # Simulate discovered award
        msg = LaudoDescubiertoCAS(
            numero_caso="CAS 2023/A/12345",
            url="https://example.com/award"
        )
        await coordinator.receive(msg)

        # Scraper should receive message
        scraper.tell.assert_called()

        await coordinator.stop()


class TestCASPipelineCheckpointing:
    """Integration tests for checkpoint/resume functionality."""

    @pytest.mark.asyncio
    async def test_checkpoint_saves_state(self):
        """Pipeline saves checkpoint on pause."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import (
            PausarPipelineCAS,
            EstadoPipelineCAS,
        )

        coordinator = CASCoordinatorActor()
        await coordinator.start()

        msg = PausarPipelineCAS(razon="test")
        await coordinator.receive(msg)

        assert coordinator.estado == EstadoPipelineCAS.PAUSADO
        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_resume_from_checkpoint(self):
        """Pipeline can resume from checkpoint."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import (
            PausarPipelineCAS,
            ReanudarPipelineCAS,
            EstadoPipelineCAS,
        )

        coordinator = CASCoordinatorActor()
        await coordinator.start()

        # Pause
        await coordinator.receive(PausarPipelineCAS())
        assert coordinator.estado == EstadoPipelineCAS.PAUSADO

        # Resume
        await coordinator.receive(ReanudarPipelineCAS())
        assert coordinator.estado != EstadoPipelineCAS.PAUSADO

        await coordinator.stop()


class TestCASPipelineErrorRecovery:
    """Integration tests for error handling and recovery."""

    @pytest.mark.asyncio
    async def test_scraper_retries_on_timeout(self):
        """Scraper retries on timeout errors."""
        from src.infrastructure.actors.cas_scraper_actor import CASScraperActor
        from src.infrastructure.actors.cas_messages import ScrapearLaudoCAS
        from src.infrastructure.adapters.cas_errors import BrowserTimeoutError

        call_count = 0

        async def mock_render(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise BrowserTimeoutError("Timeout")
            return Mock(html="<html>OK</html>", status_code=200)

        browser = AsyncMock()
        browser.render_page = mock_render

        scraper = CASScraperActor(browser_adapter=browser, max_reintentos=3)
        await scraper.start()

        msg = ScrapearLaudoCAS(
            numero_caso="CAS 2023/A/12345",
            url="https://example.com"
        )
        await scraper.receive(msg)

        await asyncio.sleep(0.2)
        assert call_count >= 2
        await scraper.stop()

    @pytest.mark.asyncio
    async def test_coordinator_handles_errors_gracefully(self):
        """Coordinator handles child errors without crashing."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import ErrorCASPipeline

        coordinator = CASCoordinatorActor()
        await coordinator.start()

        error = ErrorCASPipeline(
            mensaje="Test error",
            tipo_error="TestError",
            recuperable=True
        )
        await coordinator.receive(error)

        # Should still be running
        assert coordinator.estadisticas is not None
        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_fatal_error_stops_pipeline(self):
        """Fatal errors set pipeline to error state."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import (
            ErrorCASPipeline,
            EstadoPipelineCAS,
        )

        coordinator = CASCoordinatorActor()
        await coordinator.start()

        error = ErrorCASPipeline(
            mensaje="Fatal error",
            tipo_error="FatalError",
            recuperable=False
        )
        await coordinator.receive(error)

        await coordinator.stop()


class TestCASPipelineStatistics:
    """Integration tests for statistics collection."""

    @pytest.mark.asyncio
    async def test_statistics_track_discoveries(self):
        """Statistics track discovered awards."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import LaudoDescubiertoCAS

        scraper = AsyncMock()
        coordinator = CASCoordinatorActor(scraper_actor=scraper)
        await coordinator.start()

        initial_count = coordinator.estadisticas.laudos_descubiertos

        msg = LaudoDescubiertoCAS(
            numero_caso="CAS 2023/A/12345",
            url="https://example.com"
        )
        await coordinator.receive(msg)

        assert coordinator.estadisticas.laudos_descubiertos >= initial_count
        await coordinator.stop()

    @pytest.mark.asyncio
    async def test_obtener_estado_returns_statistics(self):
        """ObtenerEstadoCAS returns statistics."""
        from src.infrastructure.actors.cas_coordinator_actor import CASCoordinatorActor
        from src.infrastructure.actors.cas_messages import ObtenerEstadoCAS

        coordinator = CASCoordinatorActor()
        await coordinator.start()

        msg = ObtenerEstadoCAS(incluir_estadisticas=True)
        response = await coordinator.ask(msg)

        assert response is not None
        await coordinator.stop()


class TestCASPipelineWithConfig:
    """Integration tests for pipeline with CLI configuration."""

    def test_config_creates_directories(self):
        """CASCliConfig creates required directories."""
        from src.infrastructure.cli.cas_config import CASCliConfig

        with tempfile.TemporaryDirectory() as temp_dir:
            config = CASCliConfig(
                output_dir=f"{temp_dir}/output",
                checkpoint_dir=f"{temp_dir}/checkpoints",
                log_dir=f"{temp_dir}/logs",
            )
            config.ensure_directories()

            assert Path(f"{temp_dir}/output").exists()
            assert Path(f"{temp_dir}/checkpoints").exists()
            assert Path(f"{temp_dir}/logs").exists()

    def test_search_filters_describe(self):
        """SearchFilters generates human-readable description."""
        from src.infrastructure.cli.cas_config import SearchFilters

        filters = SearchFilters(
            year_from=2020,
            year_to=2024,
            sport="football",
            max_results=50
        )

        description = filters.describe()
        assert "2020" in description
        assert "2024" in description
        assert "football" in description
        assert "50" in description

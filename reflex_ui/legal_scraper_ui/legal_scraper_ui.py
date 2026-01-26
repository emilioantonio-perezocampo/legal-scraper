"""
Legal Scraper Command Center - Modern Dashboard UI.

A professional dashboard for controlling multi-source legal document scraping.
Built with Reflex + Radix UI theming for dark/light mode support.
"""
import os
from datetime import datetime
from typing import Any, Dict, List

import aiohttp
import reflex as rx


API_BASE_URL = os.getenv("SCRAPER_API_BASE_URL", "http://api:8000")


class AppState(rx.State):
    """Application state management."""

    # Auth state
    api_base_url: str = API_BASE_URL
    logged_in: bool = False
    token: str = ""
    login_username: str = ""
    login_password: str = ""
    login_error: str = ""

    # Job state
    source: str = "dof"
    status: str = "idle"
    job_id: str = ""
    progress_percent: int = 0
    progress_text: str = "0 / 0"
    status_message: str = ""
    logs: List[Dict[str, Any]] = []

    # Statistics tracking
    downloaded_count: int = 0
    processed_count: int = 0
    error_count: int = 0
    job_start_time: str = ""
    job_duration: str = "0:00"
    progress_history: List[Dict[str, Any]] = []

    # Log filtering
    log_filter: str = "all"
    log_search: str = ""

    # DOF config
    mode: str = "today"
    start_date: str = ""
    end_date: str = ""
    output_directory: str = "scraped_data"
    rate_limit_seconds: float = 2.0

    # SCJN config
    scjn_category: str = "all"
    scjn_scope: str = "all"
    scjn_max_results: int = 100
    scjn_output_directory: str = "scjn_data"

    # BJV config
    bjv_search_term: str = ""
    bjv_area: str = "all"
    bjv_max_results: int = 50
    bjv_download_pdfs: bool = True
    bjv_output_directory: str = "bjv_data"

    # Custom setters for type conversion
    def set_rate_limit_seconds(self, value: str):
        try:
            self.rate_limit_seconds = float(value) if value else 2.0
        except ValueError:
            self.rate_limit_seconds = 2.0

    def set_scjn_max_results(self, value: str):
        try:
            self.scjn_max_results = int(value) if value else 100
        except ValueError:
            self.scjn_max_results = 100

    def set_bjv_max_results(self, value: str):
        try:
            self.bjv_max_results = int(value) if value else 50
        except ValueError:
            self.bjv_max_results = 50

    def set_progress_percent(self, value: str):
        try:
            self.progress_percent = int(value) if value else 0
        except ValueError:
            self.progress_percent = 0

    @rx.var
    def filtered_logs(self) -> list[dict[str, str]]:
        """Filter logs by level and search term."""
        logs = self.logs
        if self.log_filter != "all":
            logs = [log for log in logs if log.get("level") == self.log_filter]
        if self.log_search:
            search_lower = self.log_search.lower()
            logs = [log for log in logs if search_lower in log.get("message", "").lower()]
        return logs

    @rx.var
    def chart_data(self) -> list[dict[str, Any]]:
        """Get progress history for chart."""
        return self.progress_history[-20:] if self.progress_history else []

    @rx.var
    def stats_data(self) -> list[dict[str, Any]]:
        """Get statistics data for bar chart."""
        return [
            {"name": "Downloaded", "value": self.downloaded_count},
            {"name": "Processed", "value": self.processed_count},
            {"name": "Errors", "value": self.error_count},
        ]

    async def _api_request(
        self,
        method: str,
        path: str,
        payload: Dict[str, Any] | None = None,
        use_auth: bool = True,
    ) -> Dict[str, Any]:
        """Make API request."""
        url = f"{self.api_base_url}{path}"
        headers: Dict[str, str] = {}
        if use_auth and self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.request(method, url, json=payload, headers=headers) as resp:
                if resp.content_type and "application/json" in resp.content_type:
                    data = await resp.json()
                else:
                    data = {"detail": await resp.text()}

                if resp.status == 401:
                    self.logged_in = False
                    self.token = ""
                    raise ValueError("Session expired. Please log in again.")

                if resp.status >= 400:
                    raise ValueError(data.get("detail") or data.get("error") or f"HTTP {resp.status}")

                return data

    async def login(self):
        """Handle user login."""
        self.login_error = ""
        try:
            data = await self._api_request(
                "post",
                "/api/auth/login",
                {"username": self.login_username, "password": self.login_password},
                use_auth=False,
            )
        except ValueError as exc:
            self.login_error = str(exc)
            return

        if not data.get("success"):
            self.login_error = data.get("error", "Login failed.")
            return

        self.token = data.get("access_token", "")
        self.logged_in = True
        self.login_password = ""
        await self.refresh()

    async def logout(self):
        """Handle user logout."""
        try:
            await self._api_request("post", "/api/auth/logout")
        except ValueError:
            pass
        self.logged_in = False
        self.token = ""
        self.status_message = ""
        self.logs = []
        self.progress_history = []

    async def refresh(self):
        """Refresh status and logs from API."""
        if not self.logged_in:
            return

        self.status_message = ""
        try:
            if self.source == "scjn":
                status = await self._api_request("get", "/api/scjn/status")
                logs = await self._api_request("get", "/api/scjn/logs")
                progress = status.get("progress") or {}
                downloaded = progress.get("downloaded_count", 0)
                pending = progress.get("pending_count", 0)
                total = downloaded + pending
                self.downloaded_count = downloaded
                self.processed_count = downloaded
                self.progress_percent = int((downloaded / total) * 100) if total else 0
                self.progress_text = f"{downloaded} / {total}"
            elif self.source == "bjv":
                status = await self._api_request("get", "/api/bjv/status")
                logs = await self._api_request("get", "/api/bjv/logs")
                progress = status.get("progress") or {}
                downloaded = progress.get("libros_descargados", 0)
                pending = progress.get("libros_pendientes", 0)
                total = downloaded + pending
                self.downloaded_count = downloaded
                self.processed_count = downloaded
                self.progress_percent = int((downloaded / total) * 100) if total else 0
                self.progress_text = f"{downloaded} / {total}"
            else:
                status = await self._api_request("get", "/api/status")
                logs = await self._api_request("get", "/api/logs")
                progress = status.get("progress") or {}
                percent = int(progress.get("percentage", 0))
                processed = progress.get("processed_items", 0)
                total = progress.get("total_items", 0)
                self.downloaded_count = processed
                self.processed_count = processed
                self.progress_percent = percent
                self.progress_text = f"{processed} / {total}"

            self.status = status.get("status", "idle")
            self.job_id = status.get("job_id") or ""
            self.error_count = status.get("error_count", 0)
            self.logs = logs.get("logs", [])

            # Update progress history for chart
            now = datetime.now().strftime("%H:%M")
            if not self.progress_history or self.progress_history[-1].get("time") != now:
                self.progress_history.append({
                    "time": now,
                    "progress": self.progress_percent,
                })
                if len(self.progress_history) > 30:
                    self.progress_history = self.progress_history[-30:]

        except ValueError as exc:
            self.status_message = str(exc)

    async def start_job(self):
        """Start a scraping job."""
        self.status_message = ""
        self.job_start_time = datetime.now().isoformat()
        try:
            if self.source == "scjn":
                payload = {
                    "category": self.scjn_category if self.scjn_category != "all" else None,
                    "scope": self.scjn_scope if self.scjn_scope != "all" else None,
                    "max_results": self.scjn_max_results,
                    "output_directory": self.scjn_output_directory,
                }
                await self._api_request("post", "/api/scjn/start", payload)
            elif self.source == "bjv":
                payload = {
                    "termino_busqueda": self.bjv_search_term or None,
                    "area_derecho": self.bjv_area if self.bjv_area != "all" else None,
                    "max_resultados": self.bjv_max_results,
                    "incluir_capitulos": True,
                    "descargar_pdfs": self.bjv_download_pdfs,
                    "output_directory": self.bjv_output_directory,
                }
                await self._api_request("post", "/api/bjv/start", payload)
            else:
                payload = {
                    "source": "dof",
                    "mode": self.mode,
                    "output_directory": self.output_directory,
                    "start_date": self.start_date or None,
                    "end_date": self.end_date or None,
                    "rate_limit": self.rate_limit_seconds,
                }
                await self._api_request("post", "/api/start", payload)
        except ValueError as exc:
            self.status_message = str(exc)
        await self.refresh()

    async def pause_job(self):
        """Pause the current job."""
        try:
            endpoint = "/api/pause" if self.source == "dof" else f"/api/{self.source}/pause"
            await self._api_request("post", endpoint)
        except ValueError as exc:
            self.status_message = str(exc)
        await self.refresh()

    async def resume_job(self):
        """Resume the current job."""
        try:
            endpoint = "/api/resume" if self.source == "dof" else f"/api/{self.source}/resume"
            await self._api_request("post", endpoint)
        except ValueError as exc:
            self.status_message = str(exc)
        await self.refresh()

    async def cancel_job(self):
        """Cancel the current job."""
        try:
            endpoint = "/api/cancel" if self.source == "dof" else f"/api/{self.source}/cancel"
            await self._api_request("post", endpoint)
        except ValueError as exc:
            self.status_message = str(exc)
        await self.refresh()


# =============================================================================
# COMPONENT HELPERS
# =============================================================================


def _theme_toggle() -> rx.Component:
    """Dark/light mode toggle button."""
    return rx.icon_button(
        rx.color_mode_cond(
            rx.icon("sun", size=18),
            rx.icon("moon", size=18),
        ),
        on_click=rx.toggle_color_mode,
        variant="ghost",
        size="2",
        cursor="pointer",
    )


def _stat_card(icon: str, label: str, value: rx.Var, color: str) -> rx.Component:
    """Statistics card component."""
    return rx.box(
        rx.vstack(
            rx.icon(icon, size=24, color=rx.color(color, 9)),
            rx.text(value, size="6", weight="bold"),
            rx.text(label, size="2", color=rx.color("gray", 11)),
            spacing="1",
            align="center",
        ),
        padding="1.25rem",
        background=rx.color("gray", 2),
        border_radius="12px",
        border=f"1px solid {rx.color('gray', 4)}",
        flex="1",
        min_width="140px",
    )


def _action_button(
    label: str, icon: str, on_click, color_scheme: str = "gray", variant: str = "solid"
) -> rx.Component:
    """Styled action button with icon."""
    return rx.button(
        rx.hstack(
            rx.icon(icon, size=16),
            rx.text(label),
            spacing="2",
        ),
        on_click=on_click,
        color_scheme=color_scheme,
        variant=variant,
        size="2",
        cursor="pointer",
    )


# =============================================================================
# HEADER
# =============================================================================


def _header() -> rx.Component:
    """Application header with logo and controls."""
    return rx.box(
        rx.hstack(
            rx.hstack(
                rx.icon("scale", size=28, color=rx.color("teal", 9)),
                rx.vstack(
                    rx.heading("Legal Scraper", size="5", weight="bold"),
                    rx.text("Command Center", size="1", color=rx.color("gray", 11)),
                    spacing="0",
                    align="start",
                ),
                spacing="3",
                align="center",
            ),
            rx.spacer(),
            rx.hstack(
                _theme_toggle(),
                rx.cond(
                    AppState.logged_in,
                    rx.button(
                        rx.hstack(rx.icon("log-out", size=16), rx.text("Log out"), spacing="2"),
                        on_click=AppState.logout,
                        variant="ghost",
                        size="2",
                    ),
                    rx.box(),
                ),
                spacing="2",
                align="center",
            ),
            width="100%",
            align="center",
        ),
        padding="1rem 2rem",
        background=rx.color("gray", 2),
        border_bottom=f"1px solid {rx.color('gray', 4)}",
        position="sticky",
        top="0",
        z_index="100",
        width="100%",
    )


# =============================================================================
# STATISTICS ROW
# =============================================================================


def _stats_row() -> rx.Component:
    """Row of statistics cards."""
    return rx.hstack(
        _stat_card("download", "Downloaded", AppState.downloaded_count, "teal"),
        _stat_card("check-circle", "Processed", AppState.processed_count, "blue"),
        _stat_card("alert-triangle", "Errors", AppState.error_count, "red"),
        _stat_card("clock", "Duration", AppState.job_duration, "purple"),
        spacing="4",
        width="100%",
        flex_wrap="wrap",
    )


# =============================================================================
# STATUS HERO
# =============================================================================


def _status_indicator() -> rx.Component:
    """Animated status indicator dot."""
    return rx.box(
        width="12px",
        height="12px",
        border_radius="full",
        background=rx.cond(
            AppState.status == "running",
            rx.color("green", 9),
            rx.cond(
                AppState.status == "paused",
                rx.color("amber", 9),
                rx.color("gray", 6),
            ),
        ),
        class_name=rx.cond(AppState.status == "running", "pulse-animation", ""),
    )


def _circular_progress() -> rx.Component:
    """Circular progress indicator with percentage."""
    return rx.box(
        rx.el.svg(
            rx.el.circle(
                cx="60",
                cy="60",
                r="54",
                stroke=rx.color("gray", 4),
                stroke_width="10",
                fill="none",
            ),
            rx.el.circle(
                cx="60",
                cy="60",
                r="54",
                stroke=rx.color("teal", 9),
                stroke_width="10",
                fill="none",
                stroke_dasharray="339.292",
                stroke_dashoffset=f"calc(339.292 - (339.292 * {AppState.progress_percent}) / 100)",
                stroke_linecap="round",
                transform="rotate(-90 60 60)",
                class_name="progress-ring",
            ),
            viewBox="0 0 120 120",
            width="140px",
            height="140px",
        ),
        rx.center(
            rx.vstack(
                rx.text(f"{AppState.progress_percent}%", size="6", weight="bold"),
                rx.text("complete", size="1", color=rx.color("gray", 11)),
                spacing="0",
            ),
            position="absolute",
            top="0",
            left="0",
            right="0",
            bottom="0",
        ),
        position="relative",
        width="140px",
        height="140px",
    )


def _status_hero() -> rx.Component:
    """Hero status card with circular progress."""
    return rx.box(
        rx.vstack(
            rx.hstack(
                _status_indicator(),
                rx.text(AppState.status.upper(), size="4", weight="bold"),
                spacing="2",
                align="center",
            ),
            _circular_progress(),
            rx.vstack(
                rx.text(AppState.progress_text, size="2", weight="medium"),
                rx.cond(
                    AppState.job_id != "",
                    rx.text(f"Job: {AppState.job_id}", size="1", color=rx.color("gray", 10)),
                    rx.text("No active job", size="1", color=rx.color("gray", 10)),
                ),
                spacing="1",
                align="center",
            ),
            rx.cond(
                AppState.status_message != "",
                rx.callout.root(
                    rx.callout.icon(rx.icon("circle-alert")),
                    rx.callout.text(AppState.status_message),
                    color="red",
                    size="1",
                    width="100%",
                ),
                rx.box(),
            ),
            spacing="4",
            align="center",
            width="100%",
        ),
        padding="1.5rem",
        background=rx.color("gray", 2),
        border_radius="16px",
        border=f"1px solid {rx.color('gray', 4)}",
    )


# =============================================================================
# QUICK ACTIONS
# =============================================================================


def _quick_actions() -> rx.Component:
    """Control buttons for job management."""
    return rx.box(
        rx.vstack(
            rx.text("Controls", size="3", weight="medium", color=rx.color("gray", 11)),
            rx.vstack(
                _action_button("Start Job", "play", AppState.start_job, "teal"),
                rx.hstack(
                    _action_button("Pause", "pause", AppState.pause_job, "gray", "outline"),
                    _action_button("Resume", "play", AppState.resume_job, "gray", "outline"),
                    spacing="2",
                    width="100%",
                ),
                _action_button("Cancel", "x", AppState.cancel_job, "red", "outline"),
                spacing="2",
                width="100%",
            ),
            spacing="3",
            width="100%",
        ),
        padding="1.5rem",
        background=rx.color("gray", 2),
        border_radius="12px",
        border=f"1px solid {rx.color('gray', 4)}",
    )


# =============================================================================
# SOURCE CONFIGURATION TABS
# =============================================================================


def _dof_config() -> rx.Component:
    """DOF source configuration form."""
    return rx.vstack(
        rx.text("Mode", size="2", weight="medium"),
        rx.select(
            ["today", "single", "range", "historical"],
            value=AppState.mode,
            on_change=AppState.set_mode,
            size="2",
        ),
        rx.cond(
            AppState.mode != "today",
            rx.vstack(
                rx.text("Start Date", size="2", weight="medium"),
                rx.input(type="date", value=AppState.start_date, on_change=AppState.set_start_date, size="2"),
                rx.text("End Date", size="2", weight="medium"),
                rx.input(type="date", value=AppState.end_date, on_change=AppState.set_end_date, size="2"),
                spacing="2",
                width="100%",
            ),
            rx.box(),
        ),
        rx.text("Output Directory", size="2", weight="medium"),
        rx.input(value=AppState.output_directory, on_change=AppState.set_output_directory, size="2"),
        rx.text("Rate Limit (seconds)", size="2", weight="medium"),
        rx.input(
            type="number",
            value=AppState.rate_limit_seconds,
            on_change=AppState.set_rate_limit_seconds,
            min="0.1",
            step="0.1",
            size="2",
        ),
        spacing="3",
        width="100%",
        padding="1rem",
    )


def _scjn_config() -> rx.Component:
    """SCJN source configuration form."""
    return rx.vstack(
        rx.text("Category", size="2", weight="medium"),
        rx.select(
            ["all", "LEY", "CODIGO", "REGLAMENTO", "DECRETO", "ACUERDO", "CONSTITUCION"],
            value=AppState.scjn_category,
            on_change=AppState.set_scjn_category,
            placeholder="All categories",
            size="2",
        ),
        rx.text("Scope", size="2", weight="medium"),
        rx.select(
            ["all", "FEDERAL", "ESTATAL"],
            value=AppState.scjn_scope,
            on_change=AppState.set_scjn_scope,
            placeholder="All scopes",
            size="2",
        ),
        rx.text("Max Results", size="2", weight="medium"),
        rx.input(
            type="number",
            value=AppState.scjn_max_results,
            on_change=AppState.set_scjn_max_results,
            min="1",
            size="2",
        ),
        rx.text("Output Directory", size="2", weight="medium"),
        rx.input(value=AppState.scjn_output_directory, on_change=AppState.set_scjn_output_directory, size="2"),
        spacing="3",
        width="100%",
        padding="1rem",
    )


def _bjv_config() -> rx.Component:
    """BJV source configuration form."""
    return rx.vstack(
        rx.text("Search Term", size="2", weight="medium"),
        rx.input(
            value=AppState.bjv_search_term,
            on_change=AppState.set_bjv_search_term,
            placeholder="Enter search keywords...",
            size="2",
        ),
        rx.text("Legal Area", size="2", weight="medium"),
        rx.select(
            ["all", "civil", "penal", "constitucional", "administrativo", "mercantil", "laboral", "fiscal", "internacional", "general"],
            value=AppState.bjv_area,
            on_change=AppState.set_bjv_area,
            placeholder="All areas",
            size="2",
        ),
        rx.text("Max Books", size="2", weight="medium"),
        rx.input(
            type="number",
            value=AppState.bjv_max_results,
            on_change=AppState.set_bjv_max_results,
            min="1",
            size="2",
        ),
        rx.hstack(
            rx.switch(
                checked=AppState.bjv_download_pdfs,
                on_change=AppState.set_bjv_download_pdfs,
            ),
            rx.text("Download PDFs", size="2"),
            spacing="2",
            align="center",
        ),
        rx.text("Output Directory", size="2", weight="medium"),
        rx.input(value=AppState.bjv_output_directory, on_change=AppState.set_bjv_output_directory, size="2"),
        spacing="3",
        width="100%",
        padding="1rem",
    )


def _source_tabs() -> rx.Component:
    """Tabbed interface for source selection and configuration."""
    return rx.box(
        rx.tabs.root(
            rx.tabs.list(
                rx.tabs.trigger(
                    rx.hstack(rx.icon("newspaper", size=16), rx.text("DOF"), spacing="2"),
                    value="dof",
                ),
                rx.tabs.trigger(
                    rx.hstack(rx.icon("scale", size=16), rx.text("SCJN"), spacing="2"),
                    value="scjn",
                ),
                rx.tabs.trigger(
                    rx.hstack(rx.icon("book-open", size=16), rx.text("BJV"), spacing="2"),
                    value="bjv",
                ),
                size="2",
            ),
            rx.tabs.content(_dof_config(), value="dof"),
            rx.tabs.content(_scjn_config(), value="scjn"),
            rx.tabs.content(_bjv_config(), value="bjv"),
            value=AppState.source,
            on_change=AppState.set_source,
        ),
        background=rx.color("gray", 2),
        border_radius="12px",
        border=f"1px solid {rx.color('gray', 4)}",
        overflow="hidden",
    )


# =============================================================================
# PROGRESS CHART
# =============================================================================


def _progress_chart() -> rx.Component:
    """Area chart showing progress over time."""
    return rx.box(
        rx.vstack(
            rx.text("Progress Over Time", size="3", weight="medium", color=rx.color("gray", 11)),
            rx.recharts.area_chart(
                rx.recharts.area(
                    data_key="progress",
                    fill=rx.color("teal", 4),
                    stroke=rx.color("teal", 9),
                    type_="monotone",
                ),
                rx.recharts.x_axis(data_key="time", font_size=10),
                rx.recharts.y_axis(domain=[0, 100], font_size=10),
                rx.recharts.cartesian_grid(stroke_dasharray="3 3", opacity=0.3),
                rx.recharts.tooltip(),
                data=AppState.chart_data,
                height=180,
                width="100%",
            ),
            spacing="3",
            width="100%",
        ),
        padding="1.5rem",
        background=rx.color("gray", 2),
        border_radius="12px",
        border=f"1px solid {rx.color('gray', 4)}",
    )


# =============================================================================
# ACTIVITY LOG
# =============================================================================


def _log_entry(entry: dict[str, str]) -> rx.Component:
    """Single log entry component."""
    return rx.hstack(
        rx.badge(
            entry["level"],
            color_scheme=rx.match(
                entry["level"],
                ("info", "blue"),
                ("success", "green"),
                ("warning", "amber"),
                ("error", "red"),
                "gray",
            ),
            variant="soft",
            size="1",
        ),
        rx.text(entry["message"], size="2", flex="1", style={"word_break": "break_word"}),
        rx.text(entry["timestamp"], size="1", color=rx.color("gray", 10), flex_shrink="0"),
        spacing="3",
        padding="0.5rem 0.75rem",
        border_radius="6px",
        width="100%",
        align="center",
        _hover={"background": rx.color("gray", 3)},
    )


def _activity_log() -> rx.Component:
    """Filterable activity log viewer."""
    return rx.box(
        rx.vstack(
            rx.hstack(
                rx.text("Activity Log", size="3", weight="medium"),
                rx.spacer(),
                rx.hstack(
                    rx.input(
                        placeholder="Search...",
                        value=AppState.log_search,
                        on_change=AppState.set_log_search,
                        size="1",
                        width="150px",
                    ),
                    rx.select(
                        ["all", "info", "success", "warning", "error"],
                        value=AppState.log_filter,
                        on_change=AppState.set_log_filter,
                        size="1",
                    ),
                    spacing="2",
                ),
                width="100%",
                align="center",
            ),
            rx.scroll_area(
                rx.cond(
                    AppState.filtered_logs.length() > 0,
                    rx.vstack(
                        rx.foreach(AppState.filtered_logs, _log_entry),
                        spacing="1",
                        width="100%",
                    ),
                    rx.center(
                        rx.vstack(
                            rx.icon("inbox", size=32, color=rx.color("gray", 8)),
                            rx.text("No log entries", size="2", color=rx.color("gray", 10)),
                            spacing="2",
                            align="center",
                        ),
                        padding="3rem",
                    ),
                ),
                height="280px",
                width="100%",
            ),
            spacing="3",
            width="100%",
        ),
        padding="1.5rem",
        background=rx.color("gray", 2),
        border_radius="12px",
        border=f"1px solid {rx.color('gray', 4)}",
    )


# =============================================================================
# DASHBOARD LAYOUT
# =============================================================================


def _dashboard() -> rx.Component:
    """Main dashboard layout."""
    return rx.box(
        rx.vstack(
            _stats_row(),
            rx.box(
                rx.hstack(
                    rx.vstack(
                        _status_hero(),
                        _quick_actions(),
                        spacing="4",
                        width=["100%", "100%", "35%"],
                        min_width="280px",
                    ),
                    rx.vstack(
                        _source_tabs(),
                        _progress_chart(),
                        spacing="4",
                        flex="1",
                        min_width="300px",
                    ),
                    spacing="4",
                    width="100%",
                    flex_wrap="wrap",
                    align="start",
                ),
            ),
            _activity_log(),
            spacing="4",
            width="100%",
        ),
        padding=["1rem", "1.5rem", "2rem"],
        max_width="1400px",
        margin="0 auto",
    )


# =============================================================================
# LOGIN PAGE
# =============================================================================


def _login() -> rx.Component:
    """Login form component."""
    return rx.center(
        rx.box(
            rx.vstack(
                rx.vstack(
                    rx.icon("scale", size=40, color=rx.color("teal", 9)),
                    rx.heading("Legal Scraper", size="6", weight="bold"),
                    rx.text("Sign in to access the command center", size="2", color=rx.color("gray", 11)),
                    spacing="2",
                    align="center",
                ),
                rx.vstack(
                    rx.text("Username", size="2", weight="medium"),
                    rx.input(
                        placeholder="Enter username",
                        value=AppState.login_username,
                        on_change=AppState.set_login_username,
                        size="3",
                        width="100%",
                    ),
                    rx.text("Password", size="2", weight="medium"),
                    rx.input(
                        placeholder="Enter password",
                        type="password",
                        value=AppState.login_password,
                        on_change=AppState.set_login_password,
                        size="3",
                        width="100%",
                    ),
                    spacing="2",
                    width="100%",
                ),
                rx.button(
                    rx.hstack(rx.icon("log-in", size=16), rx.text("Sign In"), spacing="2"),
                    on_click=AppState.login,
                    color_scheme="teal",
                    size="3",
                    width="100%",
                    cursor="pointer",
                ),
                rx.cond(
                    AppState.login_error != "",
                    rx.callout.root(
                        rx.callout.icon(rx.icon("circle-alert")),
                        rx.callout.text(AppState.login_error),
                        color="red",
                        size="1",
                        width="100%",
                    ),
                    rx.box(),
                ),
                spacing="5",
                width="100%",
            ),
            padding="2.5rem",
            border_radius="16px",
            background=rx.color("gray", 2),
            border=f"1px solid {rx.color('gray', 4)}",
            box_shadow="0 20px 60px rgba(0, 0, 0, 0.1)",
            max_width="400px",
            width="100%",
            class_name="fade-in",
        ),
        min_height="calc(100vh - 80px)",
        padding="2rem",
    )


# =============================================================================
# MAIN INDEX
# =============================================================================


def index() -> rx.Component:
    """Main application index."""
    return rx.box(
        rx.el.style(
            """
            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(10px); }
                to { opacity: 1; transform: translateY(0); }
            }
            @keyframes pulse {
                0%, 100% { opacity: 1; transform: scale(1); }
                50% { opacity: 0.6; transform: scale(1.1); }
            }
            .fade-in {
                animation: fadeIn 0.4s ease-out;
            }
            .pulse-animation {
                animation: pulse 2s ease-in-out infinite;
            }
            .progress-ring {
                transition: stroke-dashoffset 0.5s ease;
            }
            """
        ),
        rx.moment(interval=5000, on_change=AppState.refresh, display="none"),
        _header(),
        rx.cond(AppState.logged_in, _dashboard(), _login()),
        min_height="100vh",
        background=rx.color("gray", 1),
    )


# =============================================================================
# APP CONFIGURATION
# =============================================================================


app = rx.App(
    theme=rx.theme(
        appearance="light",
        accent_color="teal",
        gray_color="slate",
        radius="large",
        scaling="100%",
        panel_background="translucent",
    ),
    stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap",
    ],
)
app.add_page(index, title="Legal Scraper | Command Center", on_load=AppState.refresh)

# syntax=docker/dockerfile:1

# -----------------------------------------------------------------------------
# STAGE 1: Base Image & System Dependencies
# -----------------------------------------------------------------------------
# We use the 'jammy' tag to ensure a Debian/Ubuntu base, allowing apt-get.
FROM mambaorg/micromamba:1.5-jammy

# Switch to root to install system libraries required for Tkinter and X11
USER root

# Prevent tzdata prompts during apt installs
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC

# Install Tkinter system dependencies + X11 libraries + Playwright deps
# python3-tk: The actual binding
# libx11-6, libxext6: Core X11 libs
# libxrender1, libxtst6: Common GUI rendering deps
# Playwright Chromium dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    curl \
    unzip \
    python3-tk \
    libx11-6 \
    libxext6 \
    libxrender1 \
    libxtst6 \
    tk-dev \
    ca-certificates \
    gnupg \
    # Playwright/Chromium dependencies
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    # Dbus for Playwright stability
    dbus \
    dbus-x11 \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 20 LTS from NodeSource (required for Reflex)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# STAGE 2: Python Environment (Micromamba)
# -----------------------------------------------------------------------------
# Copy the environment file
COPY environment.yml /tmp/environment.yml

# Install Python dependencies into the 'base' environment
# We use the 'base' environment to avoid "conda activate" complexity in Docker
RUN micromamba install -y -n base -f /tmp/environment.yml && \
    micromamba clean --all --yes

# Activate the environment in the path for all future commands
ENV PATH="/opt/conda/bin:$PATH"

# Install Playwright browsers to a shared location accessible by all users
# Use --with-deps to ensure all system dependencies are installed
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers
RUN mkdir -p /opt/playwright-browsers && \
    chmod 755 /opt/playwright-browsers && \
    playwright install --with-deps chromium && \
    chmod -R 755 /opt/playwright-browsers

# -----------------------------------------------------------------------------
# STAGE 3: Application Setup
# -----------------------------------------------------------------------------
# Switch back to the unprivileged user provided by the base image
USER mambauser

WORKDIR /app

# Copy the application source code
# (We exclude 'tests' and 'scraped_data' via .dockerignore usually, 
#  but for the smoke test we might need tests. 
#  Best practice: Copy src/ and required files.)
COPY --chown=mambauser:mambauser src/ src/
COPY --chown=mambauser:mambauser tests/ tests/
COPY --chown=mambauser:mambauser GEMINI.md .
COPY --chown=mambauser:mambauser reflex_ui/ reflex_ui/

# Default command: Launch the CLI/TUI entry point
# "python -u" forces unbuffered stdout, essential for Docker logs
CMD ["python", "-u", "src/main.py"]

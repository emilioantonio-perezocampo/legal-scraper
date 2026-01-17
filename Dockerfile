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

# Install Tkinter system dependencies + X11 libraries
# python3-tk: The actual binding
# libx11-6, libxext6: Core X11 libs
# libxrender1, libxtst6: Common GUI rendering deps
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

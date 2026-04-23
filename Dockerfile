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
ENV TESSDATA_PREFIX=/usr/share/tesseract-ocr/4.00/tessdata/
ENV SCRAPER_DOCLING_ARTIFACTS_PATH=/opt/docling-artifacts
ENV DOCLING_SERVE_ARTIFACTS_PATH=/opt/docling-artifacts

# Install Tkinter system dependencies + X11 libraries + Playwright deps
# python3-tk: The actual binding
# libx11-6, libxext6: Core X11 libs
# libxrender1, libxtst6: Common GUI rendering deps
# Playwright Chromium dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
    antiword \
    curl \
    unzip \
    libmagic1 \
    poppler-utils \
    tesseract-ocr \
    tesseract-ocr-eng \
    tesseract-ocr-fra \
    tesseract-ocr-spa \
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
    libgl1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    # Dbus for Playwright stability
    dbus \
    dbus-x11 \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime \
    && echo $TZ > /etc/timezone \
    && apt-get install -y --no-install-recommends tesseract-ocr-fra \
    && test -f /usr/share/tesseract-ocr/4.00/tessdata/fra.traineddata \
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

# Pre-install the spaCy model Unstructured uses for HTML tokenization so the
# worker never tries to write into site-packages at runtime.
RUN python -m spacy download en_core_web_sm

# RapidOCR downloads local model artifacts on first use. Ensure the package
# cache path is writable for the unprivileged runtime user so Docling can stay
# fully self-hosted without permission errors.
RUN mkdir -p /opt/conda/lib/python3.11/site-packages/rapidocr/models && \
    chown -R mambauser:mambauser /opt/conda/lib/python3.11/site-packages/rapidocr

# Prefetch the subset of Docling models the local PDF benchmark paths actually
# use. `--all` also pulls large VLM bundles that are not part of the CAS/Biblio
# benchmark lanes and make the image unnecessarily heavy.
RUN mkdir -p /opt/docling-artifacts && \
    docling-tools models download -q -o /opt/docling-artifacts layout tableformer rapidocr && \
    chown -R mambauser:mambauser /opt/docling-artifacts

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

# Warm local PDF benchmark dependencies during the image build so first-use
# CAS/Biblio benchmark runs do not fetch OCR or layout artifacts at runtime.
RUN python - <<'PY'
from pathlib import Path
import fitz

tmp_dir = Path("/tmp/benchmark_warmup")
tmp_dir.mkdir(parents=True, exist_ok=True)
pdf_path = tmp_dir / "warmup.pdf"

doc = fitz.open()
page = doc.new_page()
page.insert_text(
    (72, 72),
    "CAS benchmark warmup document.\n"
    "This PDF exists only to prefetch local OCR and layout artifacts.\n"
    "Le present document sert uniquement a prechauffer les dependances locales.",
)
doc.save(pdf_path)
doc.close()

from unstructured.partition.pdf import partition_pdf

partition_pdf(
    filename=str(pdf_path),
    strategy="fast",
    languages=["eng", "fra"],
)

from src.gui.infrastructure.cas_chunk_benchmark import _build_docling_converter

converter = _build_docling_converter(enable_remote_services=False)
converter.convert(source=str(pdf_path))
PY

# Default command: Launch the CLI/TUI entry point
# "python -u" forces unbuffered stdout, essential for Docker logs
CMD ["python", "-u", "src/main.py"]

# ── Build stage ──────────────────────────────────────────────────────────────
FROM python:3.12-slim

# System dependencies:
#   poppler-utils  → pdf2image (PDF → image conversion)
#   tesseract-ocr  → pytesseract (OCR for scanned PDFs)
#   tesseract-ocr-eng → English language pack for tesseract
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    tesseract-ocr \
    tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cached layer if requirements unchanged)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create required directories (volumes will mount over these, but needed for
# first-run without volumes)
RUN mkdir -p data uploads static

# Don't run as root
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8888

CMD ["python3", "server.py"]

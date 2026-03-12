#!/bin/bash
# TIFP Mill Lists - Trading Intelligence Tool
# Start the application server

cd "$(dirname "$0")"

# Optional: Set OpenAI API key for AI-powered parsing
# export OPENAI_API_KEY="your-key-here"

# Set port (default 8888)
export PORT=${PORT:-8888}

echo ""
echo "=============================================="
echo "  TIFP Mill Lists - Trading Intelligence"
echo "  Starting on http://localhost:$PORT"
echo "=============================================="
echo ""

python3 server.py

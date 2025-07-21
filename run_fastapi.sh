#!/bin/bash
# Run FastAPI server with proper environment

cd "$(dirname "$0")"
export PYTHONPATH="$PWD/src:$PYTHONPATH"

echo "Starting AEMO Dashboard FastAPI server..."
echo "API docs will be available at: http://localhost:8000/docs"

# Run with miniforge python
/Users/davidleitch/miniforge3/bin/python src/main_fastapi.py
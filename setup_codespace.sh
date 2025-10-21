#!/bin/bash
set -e  # Exit on error

# Check if venv exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
else
    echo "Virtual environment already exists. Skipping creation."
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing requirements..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Loading credentials..."
python load_credentials.py

echo "Setup complete."

# Use this file only on GitHub's codespace
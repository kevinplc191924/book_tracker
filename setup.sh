#!/bin/bash
set -e  # Exit on error

echo "Installing requirements..."
pip install -r requirements.txt

echo "Loading credentials..."
python load_credentials.py

echo "Setup complete."
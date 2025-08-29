"""EDGAR Extraction Toolkit crawler modules."""

import os

# Directory constants for the integrated system
DATASET_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), 'data')
LOGGING_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), 'logs')

# Ensure directories exist
if not os.path.exists(DATASET_DIR):
    os.makedirs(DATASET_DIR, exist_ok=True)

if not os.path.exists(LOGGING_DIR):
    os.makedirs(LOGGING_DIR, exist_ok=True)
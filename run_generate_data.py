"""
Quick script to generate sample data without dependencies on terminal.
"""
import sys
import os

# Add scripts directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'scripts'))

# Import and run the generation
from generate_sample_data import main

if __name__ == "__main__":
    print("="*60)
    print("Generating sample e-commerce data...")
    print("="*60)
    main()

#!/usr/bin/env python
import sys
import os
import argparse
from src.etl.extract.csv_extractor import CsvExtractor

def main():
    parser = argparse.ArgumentParser(description='Extract data from CSV to staging')
    parser.add_argument('--input-file', required=True, help='Path to input CSV file')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    
    print(f"Starting extraction from {args.input_file}")
    extractor = CsvExtractor(args.input_file)
    records = extractor.extract()
    print(f"Extraction complete: {records} records processed")

if __name__ == "__main__":
    main()

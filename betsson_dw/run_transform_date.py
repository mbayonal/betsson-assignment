#!/usr/bin/env python
import sys
import argparse
from src.etl.transform.date_transformer import DateTransformer

def main():
    parser = argparse.ArgumentParser(description='Transform dates from staging into date dimension')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    
    if args.verbose:
        print("Starting date transformation...")
    
    transformer = DateTransformer()
    dates_processed = transformer.transform_dates()
    
    if args.verbose:
        print(f"Date transformation complete: {dates_processed} dates processed")

if __name__ == "__main__":
    main()

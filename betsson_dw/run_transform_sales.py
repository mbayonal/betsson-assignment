#!/usr/bin/env python
import sys
import argparse
from src.etl.transform.sales_transformer import SalesTransformer

def main():
    parser = argparse.ArgumentParser(description='Transform sales from staging into fact table')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    
    if args.verbose:
        print(f"Starting sales transformation with batch size {args.batch_size}...")
    
    transformer = SalesTransformer()
    sales_processed = transformer.transform_sales(batch_size=args.batch_size)
    
    if args.verbose:
        print(f"Sales transformation complete: {sales_processed} sales processed")

if __name__ == "__main__":
    main()

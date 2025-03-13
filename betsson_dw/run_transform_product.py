#!/usr/bin/env python
import sys
import argparse
from src.etl.transform.product_transformer import ProductTransformer

def main():
    parser = argparse.ArgumentParser(description='Transform products from staging into product dimension')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    
    if args.verbose:
        print("Starting product transformation...")
    
    transformer = ProductTransformer()
    products_processed = transformer.transform_products()
    
    if args.verbose:
        print(f"Product transformation complete: {products_processed} products processed")

if __name__ == "__main__":
    main()

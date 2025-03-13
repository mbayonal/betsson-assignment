#!/usr/bin/env python
import sys
import argparse
from src.etl.transform.customer_transformer import CustomerTransformer

def main():
    parser = argparse.ArgumentParser(description='Transform customers from staging into customer dimension')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    
    if args.verbose:
        print("Starting customer transformation...")
    
    transformer = CustomerTransformer()
    customers_processed = transformer.transform_customers()
    
    if args.verbose:
        print(f"Customer transformation complete: {customers_processed} customers processed")

if __name__ == "__main__":
    main()

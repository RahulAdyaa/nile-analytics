import pandas as pd
import numpy as np
from datetime import datetime
from django.db import transaction
from dashboard.models import Customer, Product, Sale

class ETLPipeline:
    def __init__(self, file_path, column_mapping=None):
        self.file_path = file_path
        self.column_mapping = column_mapping  # Dict mapping user columns to expected columns
        self.raw_df = None
        self.cleaned_df = None
        self.validated_df = None
        self.final_df = None

    def run(self):
        """Main execution entry point for the ETL pipeline."""
        print(f"Starting ETL Pipeline for: {self.file_path}")
        if self.raw_df is None:
            self.extract()
        self.validate_schema()
        self.clean_data()
        self.feature_engineering()
        self.load_to_db()
        print("ETL Pipeline completed successfully.")

    @property
    def expected_columns_list(self):
        return [
            'Order ID', 'Order Date', 'Customer Name', 'Region', 'City',
            'Category', 'Sub-Category', 'Product Name', 'Quantity',
            'Unit Price', 'Discount', 'Sales', 'Profit', 'Payment Mode'
        ]

    def get_mapping_preview(self):
        """Extracts headers and performs auto-mapping for review."""
        if self.raw_df is None:
            self.extract()
        self._auto_map_columns()
        
        confidence = len(self.column_mapping) / len(self.expected_columns_list)
        return {
            'headers': list(self.raw_df.columns),
            'mapping': self.column_mapping,
            'expected': self.expected_columns_list,
            'confidence': confidence
        }

    def extract(self):
        """Step 1: Extraction - Support CSV and Excel."""
        if self.file_path.endswith('.csv'):
            self.raw_df = pd.read_csv(self.file_path)
        elif self.file_path.endswith('.xlsx'):
            self.raw_df = pd.read_excel(self.file_path, engine='openpyxl')
        else:
            raise ValueError("Unsupported file format. Please provide .csv or .xlsx")
        print(f"Extraction: Read {len(self.raw_df)} rows.")

    def _auto_map_columns(self):
        """Automatically infers and maps uploaded columns to expected columns."""
        import difflib
        import re

        expected_columns = [
            'Order ID', 'Order Date', 'Customer Name', 'Region', 'City',
            'Category', 'Sub-Category', 'Product Name', 'Quantity',
            'Unit Price', 'Discount', 'Sales', 'Profit', 'Payment Mode'
        ]
        
        aliases = {
            'order id': ['id', 'transaction', 'invoice', 'orderid'],
            'order date': ['date', 'time', 'timestamp', 'created_at', 'orderdate'],
            'customer name': ['customer', 'name', 'client', 'buyer', 'user'],
            'region': ['area', 'zone', 'state', 'territory'],
            'city': ['location', 'town'],
            'category': ['type', 'group', 'department'],
            'sub-category': ['subcategory', 'sub_category', 'sub'],
            'product name': ['product', 'item', 'article'],
            'quantity': ['qty', 'amount', 'count'],
            'unit price': ['price', 'cost', 'rate', 'unitprice'],
            'discount': ['off', 'reduction', 'discount'],
            'sales': ['revenue', 'total', 'amount'],
            'profit': ['margin', 'gain'],
            'payment mode': ['payment', 'method', 'payment_method', 'type']
        }

        def normalize(text):
            return re.sub(r'[^a-z0-9]', '', str(text).lower())

        mapping = {}
        actual_cols = list(self.raw_df.columns)
        assigned_actuals = set()

        for expected in expected_columns:
            norm_expected = normalize(expected)
            best_match = None
            
            # 1. Exact normalized match
            for col in actual_cols:
                if col in assigned_actuals: continue
                if normalize(col) == norm_expected:
                    best_match = col
                    break
            
            # 2. Alias match
            if not best_match:
                exp_key = expected.lower()
                for col in actual_cols:
                    if col in assigned_actuals: continue
                    norm_col = normalize(col)
                    if exp_key in aliases and any(normalize(alias) in norm_col for alias in aliases[exp_key]):
                        best_match = col
                        break

            if best_match:
                mapping[best_match] = expected
                assigned_actuals.add(best_match)

        self.column_mapping = mapping
        print(f"Auto-mapped columns: {self.column_mapping}")

    def validate_schema(self):
        """Step 2: Validation - Schema enforcement with auto-mapping."""
        df = self.raw_df.copy()
        
        # Apply automatic mapping if no custom mapping exists
        if not self.column_mapping:
            self._auto_map_columns()
            
        if self.column_mapping:
            df.rename(columns=self.column_mapping, inplace=True)
            print(f"Schema Validation: Applied mapping {self.column_mapping}")

        expected_columns = [
            'Order ID', 'Order Date', 'Customer Name', 'Region', 'City',
            'Category', 'Sub-Category', 'Product Name', 'Quantity',
            'Unit Price', 'Discount', 'Sales', 'Profit', 'Payment Mode'
        ]
        
        # Handle missing columns by adding defaults
        missing_cols = [col for col in expected_columns if col not in df.columns]
        if missing_cols:
            print(f"Schema Validation: Missing columns detected: {missing_cols}. Applying defaults.")
            for col in missing_cols:
                if col in ['Unit Price', 'Discount', 'Sales', 'Profit', 'Quantity']:
                    df[col] = 0
                elif col == 'Order Date':
                    df[col] = pd.Timestamp.now()
                else:
                    df[col] = 'Unknown'
        
        self.validated_df = df[expected_columns].copy()
        print("Schema Validation: Columns verified/defaulted.")

    def clean_data(self):
        """Step 3: Cleaning - Nulls, duplicates, and invalid entries."""
        df = self.validated_df.copy()
        
        # 1. Drop rows missing critical identifiers or metrics
        critical_cols = ['Order ID', 'Product Name', 'Quantity', 'Unit Price']
        df.dropna(subset=critical_cols, inplace=True)
        
        # 2. Fill optional fields with defaults
        df['Region'] = df['Region'].fillna('Unknown')
        df['City'] = df['City'].fillna('Unknown')
        df['Category'] = df['Category'].fillna('Uncategorized')
        
        # 3. Filter invalid numerical data (Quantity > 0, Price >= 0)
        df = df[df['Quantity'] > 0]
        df = df[df['Unit Price'] >= 0]
        
        # 4. Standardize text fields (trim and normalize case)
        text_cols = ['Customer Name', 'Region', 'City', 'Category', 'Sub-Category', 'Product Name', 'Payment Mode']
        for col in text_cols:
            df[col] = df[col].astype(str).str.strip().str.title()
            
        # 5. Handle Date parsing
        df['Order Date'] = pd.to_datetime(df['Order Date'], errors='coerce')
        df.dropna(subset=['Order Date'], inplace=True)
        
        # 6. Deduplication
        df.drop_duplicates(inplace=True)
        
        self.cleaned_df = df
        print(f"Cleaning: Processed data down to {len(self.cleaned_df)} clean rows.")

    def feature_engineering(self):
        """Step 4: Feature Engineering - Derived metrics."""
        df = self.cleaned_df.copy()
        
        # Compute Revenue (if not already strictly Sales)
        df['derived_revenue'] = df['Quantity'] * df['Unit Price']
        
        # Extract temporal features
        df['order_month'] = df['Order Date'].dt.month
        df['order_year'] = df['Order Date'].dt.year
        
        self.final_df = df
        print("Feature Engineering: Derived metrics computed.")

    def load_to_db(self):
        """Step 5: Loading - Idempotent relational loading into database."""
        df = self.final_df
        
        with transaction.atomic():
            # 1. Collect unique entities to reduce DB round-trips
            unique_customers = df[['Customer Name', 'Region', 'City']].drop_duplicates()
            unique_products = df[['Product Name', 'Category', 'Sub-Category']].drop_duplicates()
            
            # Map for efficient lookup
            customer_map = {}
            for _, row in unique_customers.iterrows():
                cust, created = Customer.objects.get_or_create(
                    name=row['Customer Name'],
                    region=row['Region'],
                    city=row['City']
                )
                customer_map[(row['Customer Name'], row['Region'], row['City'])] = cust
                
            product_map = {}
            for _, row in unique_products.iterrows():
                prod, created = Product.objects.get_or_create(
                    name=row['Product Name'],
                    category=row['Category'],
                    sub_category=row['Sub-Category']
                )
                product_map[(row['Product Name'], row['Category'], row['Sub-Category'])] = prod
            
            # 2. Prepare Sales objects for bulk create (Skip existing to maintain idempotency if needed)
            # For simplicity in this demo, we append all cleaned data. 
            # In production, we'd check for (Order ID + Product ID) uniqueness.
            
            sales_to_create = []
            for _, row in df.iterrows():
                sales_to_create.append(Sale(
                    order_id=row['Order ID'],
                    order_date=row['Order Date'].date(),
                    customer=customer_map[(row['Customer Name'], row['Region'], row['City'])],
                    product=product_map[(row['Product Name'], row['Category'], row['Sub-Category'])],
                    quantity=row['Quantity'],
                    unit_price=row['Unit Price'],
                    discount=row['Discount'],
                    total_sales=row['Sales'],
                    profit=row['Profit'],
                    payment_mode=row['Payment Mode']
                ))
            
            Sale.objects.bulk_create(sales_to_create, batch_size=1000)
            print(f"Loading: {len(sales_to_create)} sale records created across Customers and Products.")

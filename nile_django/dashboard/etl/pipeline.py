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

    def run(self, wipe_existing=False):
        """Main execution entry point for the ETL pipeline."""
        print(f"Starting ETL Pipeline for: {self.file_path}")
        if self.raw_df is None:
            self.extract()
        self.validate_schema()
        self.clean_data()
        self.feature_engineering()
        self.load_to_db(wipe_existing=wipe_existing)
        print("ETL Pipeline completed successfully.")

    @property
    def expected_columns_list(self):
        return [
            'Order ID', 'Order Date', 'Customer ID', 'Customer Name', 'Region', 'City',
            'Product ID', 'Category', 'Sub-Category', 'Product Name', 'Quantity',
            'Unit Price', 'Discount', 'Sales', 'Profit', 'Payment Mode',
            'Delivery Time', 'Returned', 'Shipping Cost', 'Age', 'Gender'
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
        """Automatically infers and maps uploaded columns using multi-level intelligent matching."""
        import difflib
        import re

        expected_columns = self.expected_columns_list
        
        # Expanded Intelligent Alias Library
        # Includes Django ORM-style names (customer__region) from our own exports
        aliases = {
            'order id': ['id', 'transaction', 'invoice', 'orderid', 'order_no', 'ref_id', 'trans_id', 'order_id'],
            'order date': ['date', 'time', 'timestamp', 'created_at', 'orderdate', 'transaction_date', 'purchase_date', 'order_date'],
            'customer id': ['cust_id', 'customerid', 'client_id', 'user_id', 'buyer_id', 'customer_id'],
            'customer name': ['customer', 'name', 'client', 'buyer', 'user', 'purchaser', 'full_name', 'contact_name', 'customer__name'],
            'region': ['area', 'zone', 'state', 'territory', 'province', 'distict', 'customer__region'],
            'city': ['location', 'town', 'municipality', 'shipping_city', 'customer__city'],
            'product id': ['prod_id', 'productid', 'sku', 'item_id', 'article_id', 'product_id'],
            'category': ['type', 'group', 'department', 'class', 'division', 'product__category'],
            'sub-category': ['subcategory', 'sub_category', 'sub', 'sub_class', 'sub_group', 'product__sub_category'],
            'product name': ['product', 'item', 'article', 'description', 'sku_name', 'item_name', 'product__name'],
            'quantity': ['qty', 'count', 'units', 'volume'],
            'unit price': ['price', 'cost', 'rate', 'unitprice', 'msrp', 'list_price', 'unit_price'],
            'discount': ['off', 'reduction', 'rebate', 'promo', 'markdown'],
            'sales': ['revenue', 'total_sales', 'total_amount', 'line_total', 'gross_sales', 'net_sales'],
            'profit': ['margin', 'gain', 'profit_margin', 'net_profit', 'earnings'],
            'payment mode': ['payment', 'method', 'payment_method', 'pay_type', 'pay_mode', 'payment_mode'],
            'delivery time': ['delivery', 'days', 'delivery_time_days', 'shipping_days', 'transit_time'],
            'returned': ['returned', 'return', 'refunded', 'is_returned'],
            'shipping cost': ['shipping', 'shipping_cost', 'freight', 'delivery_fee'],
            'age': ['age', 'customer_age', 'user_age', 'customer__age'],
            'gender': ['gender', 'customer_gender', 'sex', 'user_gender', 'customer__gender']
        }

        def normalize(text):
            return re.sub(r'[^a-z0-9]', '', str(text).lower())

        mapping = {}
        actual_cols = list(self.raw_df.columns)
        assigned_actuals = set()

        for expected in expected_columns:
            norm_expected = normalize(expected)
            best_match = None
            
            # Level 1: Exact / Normalized Match
            for col in actual_cols:
                if col in assigned_actuals: continue
                if normalize(col) == norm_expected:
                    best_match = col
                    break
            
            # Level 2: Comprehensive Alias Match
            if not best_match:
                exp_key = expected.lower()
                if exp_key in aliases:
                    for col in actual_cols:
                        if col in assigned_actuals: continue
                        norm_col = normalize(col)
                        if any(normalize(alias) == norm_col for alias in aliases[exp_key]):
                            best_match = col
                            break
            
            # Level 3: Fuzzy / Similarity Match (Levenshtein Distance)
            # Increased cutoff to 0.85 to prevent "customer_age" mapping to "customer name"
            if not best_match:
                matches = difflib.get_close_matches(normalize(expected), [normalize(c) for c in actual_cols if c not in assigned_actuals], n=1, cutoff=0.85)
                if matches:
                    # Find back the original column name from the normalized match
                    for col in actual_cols:
                        if col in assigned_actuals: continue
                        if normalize(col) == matches[0]:
                            best_match = col
                            break

            if best_match:
                mapping[best_match] = expected
                assigned_actuals.add(best_match)

        self.column_mapping = mapping
        print(f"Intelligent Mapping: Resulting Map -> {self.column_mapping}")

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
            'Order ID', 'Order Date', 'Customer ID', 'Customer Name', 'Region', 'City',
            'Product ID', 'Category', 'Sub-Category', 'Product Name', 'Quantity',
            'Unit Price', 'Discount', 'Sales', 'Profit', 'Payment Mode',
            'Delivery Time', 'Returned', 'Shipping Cost', 'Age', 'Gender'
        ]
        
        # Handle missing columns by adding defaults
        missing_cols = [col for col in expected_columns if col not in df.columns]
        if missing_cols:
            print(f"Schema Validation: Missing columns detected: {missing_cols}. Applying defaults.")
            for col in missing_cols:
                if col in ['Unit Price', 'Discount', 'Sales', 'Profit', 'Quantity', 'Delivery Time', 'Shipping Cost', 'Age']:
                    df[col] = 0
                elif col == 'Order Date':
                    df[col] = pd.Timestamp.now()
                elif col == 'Returned':
                    df[col] = False
                elif col in ['Customer ID', 'Product ID']:
                    df[col] = ''
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
        
        # 6. Boolean Conversion for 'Returned'
        if 'Returned' in df.columns:
            # Map various true/false strings to boolean
            df['Returned'] = df['Returned'].astype(str).str.lower().map({
                'yes': True, 'no': False, 
                '1': True, '0': False, 
                'true': True, 'false': False,
                '1.0': True, '0.0': False
            })
            df['Returned'] = df['Returned'].fillna(False)
        
        # 7. Deduplication
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

    def load_to_db(self, wipe_existing=False):
        """Step 5: Loading - Idempotent relational loading into database."""
        df = self.final_df
        
        with transaction.atomic():
            if wipe_existing:
                print("Wiping existing data for a clean ingestion...")
                Sale.objects.all().delete()
                Customer.objects.all().delete()
                Product.objects.all().delete()
            
            # 1. Collect unique entities to reduce DB round-trips
            unique_customers = df[['Customer ID', 'Customer Name', 'Region', 'City', 'Age', 'Gender']].drop_duplicates(subset=['Customer Name', 'Region', 'City'])
            unique_products = df[['Product ID', 'Product Name', 'Category', 'Sub-Category']].drop_duplicates(subset=['Product Name', 'Category', 'Sub-Category'])
            
            # Map for efficient lookup
            customer_map = {}
            for _, row in unique_customers.iterrows():
                cust, created = Customer.objects.get_or_create(
                    name=row['Customer Name'],
                    region=row['Region'],
                    city=row['City']
                )
                # Update attributes if they are new or missing
                needs_save = False
                
                cust_id = str(row['Customer ID']).strip() if pd.notna(row['Customer ID']) else ''
                if cust_id and cust_id.lower() != 'nan' and not cust.customer_id:
                    cust.customer_id = cust_id
                    needs_save = True
                    
                if row['Age'] != 0 and (cust.age is None or cust.age == 0):
                    cust.age = row['Age']
                    needs_save = True
                if row['Gender'] != 'Unknown' and (not cust.gender or cust.gender == 'Unknown'):
                    cust.gender = row['Gender']
                    needs_save = True
                
                if needs_save:
                    cust.save()
                
                customer_map[(row['Customer Name'], row['Region'], row['City'])] = cust
                
            product_map = {}
            for _, row in unique_products.iterrows():
                prod, created = Product.objects.get_or_create(
                    name=row['Product Name'],
                    category=row['Category'],
                    sub_category=row['Sub-Category']
                )
                
                prod_id = str(row['Product ID']).strip() if pd.notna(row['Product ID']) else ''
                if prod_id and prod_id.lower() != 'nan' and not prod.product_id:
                    prod.product_id = prod_id
                    prod.save()
                    
                product_map[(row['Product Name'], row['Category'], row['Sub-Category'])] = prod
            
            # 2. Prepare Sales objects for bulk create
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
                    payment_mode=row['Payment Mode'],
                    delivery_time_days=row['Delivery Time'],
                    returned=row['Returned'],
                    shipping_cost=row['Shipping Cost']
                ))
            
            Sale.objects.bulk_create(sales_to_create, batch_size=1000)
            print(f"Loading: {len(sales_to_create)} sale records created across Customers and Products.")

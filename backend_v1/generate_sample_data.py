import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sample_data(filename="sample_sales.csv", num_rows=1000):
    products = {
        "Nile-Pad Pro": ("Electronics", 799),
        "Nile-Buds Air": ("Electronics", 199),
        "Eco-Tee Organic": ("Apparel", 25),
        "Terra-Flask 1L": ("Home", 35),
        "Smart-Watch X": ("Electronics", 299),
        "Cloud-Walker Shoes": ("Apparel", 120),
        "Lumina Desk Lamp": ("Home", 45),
    }
    
    regions = ["North America", "Europe", "Asia-Pacific", "Latin America"]
    product_names = list(products.keys())
    
    data = []
    start_date = datetime(2025, 1, 1)
    
    for i in range(num_rows):
        p_name = np.random.choice(product_names)
        category, price = products[p_name]
        qty = np.random.randint(1, 5)
        order_date = start_date + timedelta(days=np.random.randint(0, 365))
        
        data.append({
            "Order ID": f"ORD-{1000 + i}",
            "Date": order_date.strftime("%Y-%m-%d"),
            "Customer ID": f"CUST-{np.random.randint(1, 200)}",
            "Product Name": p_name,
            "Category": category,
            "Quantity": qty,
            "Price": price,
            "Region": np.random.choice(regions)
        })
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Sample data generated: {filename}")

if __name__ == "__main__":
    generate_sample_data()

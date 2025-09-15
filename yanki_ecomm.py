import numpy as np
import pandas as pd
import psycopg2
import csv

# --- Data Cleaning ---
yanki_df = pd.read_csv(r"C:\Users\Godwin\Desktop\yanki_ecommerce_casestudy\yanki_ecommerce_case_study\dataSet\rawdata\yanki_ecommerce.csv")

# Drop missing values
yanki_df.dropna(subset=['Order_ID', 'Customer_ID'], inplace=True)

# Convert Order_Date
yanki_df['Order_Date'] = pd.to_datetime(yanki_df['Order_Date'], errors='coerce')
bad_dates = yanki_df[yanki_df['Order_Date'].isna()]
print("Bad Dates:", bad_dates['Order_Date'].unique())

# Force consistent format
yanki_df['Order_Date'] = pd.to_datetime(yanki_df['Order_Date'], format="%d/%m/%Y", errors='coerce')

# Customer table
customer_df = (
    yanki_df[['Customer_ID', 'Customer_Name', 'Email', 'Phone_Number']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Product table
products_df = (
    yanki_df[['Product_ID', 'Product_Name', 'Brand', 'Category', 'Price']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Shipping address table
shipping_address_df = (
    yanki_df[['Customer_ID', 'Shipping_Address', 'City', 'State', 'Country', 'Postal_Code']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Order table
order_df = (
    yanki_df[['Order_ID', 'Customer_ID', 'Product_ID', 'Quantity', 'Total_Price', 'Order_Date']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Payment method table
payment_method_df = (
    yanki_df[['Order_ID', 'Payment_Method', 'Transaction_Status']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Save cleaned CSVs
customer_df.to_csv(r"dataSet\cleandata\customer.csv", index=False)
products_df.to_csv(r"dataSet\cleandata\products.csv", index=False)
order_df.to_csv(r"dataSet\cleandata\order.csv", index=False)
shipping_address_df.to_csv(r"dataSet\cleandata\shipping_address.csv", index=False)
payment_method_df.to_csv(r"dataSet\cleandata\payment_method.csv", index=False)

# --- Database Connection ---
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="yanki_ecommerce",
        user="postgres",
        password="godwin@76"
    )

# --- Create Tables ---
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_tables_query = '''
        CREATE SCHEMA IF NOT EXISTS yanki;

        DROP TABLE IF EXISTS yanki.customer CASCADE;
        DROP TABLE IF EXISTS yanki.products CASCADE;
        DROP TABLE IF EXISTS yanki.shipping_address CASCADE;
        DROP TABLE IF EXISTS yanki."order" CASCADE;
        DROP TABLE IF EXISTS yanki.payment_method CASCADE;

        CREATE TABLE yanki.customer (
            Customer_ID TEXT PRIMARY KEY,
            Customer_Name TEXT,
            Email TEXT,
            Phone_Number TEXT
        );

        CREATE TABLE yanki.products (
            Product_ID TEXT PRIMARY KEY,
            Product_Name TEXT,
            Brand TEXT,
            Category TEXT,
            Price FLOAT
        );

        CREATE TABLE yanki.shipping_address (
            Shipping_ID SERIAL PRIMARY KEY,
            Customer_ID TEXT,
            Shipping_Address TEXT,
            City TEXT,
            State TEXT,
            Country TEXT,
            Postal_Code TEXT,
            FOREIGN KEY (Customer_ID) REFERENCES yanki.customer(Customer_ID)
        );

        CREATE TABLE yanki."order" (
            Order_ID TEXT PRIMARY KEY,
            Customer_ID TEXT,
            Product_ID TEXT,
            Quantity INTEGER,
            Total_Price FLOAT,
            Order_Date DATE,
            FOREIGN KEY (Customer_ID) REFERENCES yanki.customer(Customer_ID),
            FOREIGN KEY (Product_ID) REFERENCES yanki.products(Product_ID)
        );

        CREATE TABLE yanki.payment_method (
            Order_ID TEXT,
            Payment_Method TEXT,
            Transaction_Status TEXT,
            FOREIGN KEY (Order_ID) REFERENCES yanki."order"(Order_ID)
        );
    '''
    cursor.execute(create_tables_query)
    conn.commit()
    cursor.close()
    conn.close()

create_tables()

# --- Generic Loader ---
def load_csv_into_table(csv_path, insert_sql):
    conn = get_db_connection()
    cursor = conn.cursor()
    with open(csv_path, 'r') as file:
        reader = csv.reader(file)
        next(reader)  # skip header
        for row in reader:
            cursor.execute(insert_sql, row)
    conn.commit()
    cursor.close()
    conn.close()

# Insert data
load_csv_into_table(r"dataSet\cleandata\customer.csv",
    '''INSERT INTO yanki.customer (Customer_ID, Customer_Name, Email, Phone_Number)
       VALUES (%s, %s, %s, %s)
       ON CONFLICT (Customer_ID) DO NOTHING''')

load_csv_into_table(r"dataSet\cleandata\products.csv",
    '''INSERT INTO yanki.products (Product_ID, Product_Name, Brand, Category, Price)
       VALUES (%s, %s, %s, %s, %s)
       ON CONFLICT (Product_ID) DO NOTHING''')

load_csv_into_table(r"dataSet\cleandata\shipping_address.csv",
    '''INSERT INTO yanki.shipping_address (Customer_ID, Shipping_Address, City, State, Country, Postal_Code)
       VALUES (%s, %s, %s, %s, %s, %s)''')

load_csv_into_table(r"dataSet\cleandata\order.csv",
    '''INSERT INTO yanki."order" (Order_ID, Customer_ID, Product_ID, Quantity, Total_Price, Order_Date)
       VALUES (%s, %s, %s, %s, %s, %s)
       ON CONFLICT (Order_ID) DO NOTHING''')

load_csv_into_table(r"dataSet\cleandata\payment_method.csv",
    '''INSERT INTO yanki.payment_method (Order_ID, Payment_Method, Transaction_Status)
       VALUES (%s, %s, %s)''')





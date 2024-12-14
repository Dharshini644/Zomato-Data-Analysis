import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

db = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="12345678",
    database="guvidb"
)
c = db.cursor()

'''
c.execute("""
CREATE TABLE IF NOT EXISTS Customers_table (
    customer_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(15),
    location VARCHAR(150),
    signup_date DATE,
    is_premium BOOLEAN,
    preferred_cuisine VARCHAR(25),
    total_orders INT,
    average_rating DOUBLE
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS Restaurants_table (
    restaurant_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(70),
    cuisine_type VARCHAR(15),
    location VARCHAR(150),
    owner_name VARCHAR(50),
    average_delivery_time INT,
    contact_number VARCHAR(15),
    rating DOUBLE,
    total_orders INT,
    is_active BOOLEAN
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS Orders_table (
    order_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    restaurant_id VARCHAR(100),
    order_date DATETIME,
    delivery_time DATETIME,
    status VARCHAR(10),
    total_amount DOUBLE,
    payment_mode VARCHAR(15),
    discount_applied INT,
    feedback_rating DOUBLE,
    FOREIGN KEY (customer_id) REFERENCES Customers_table(customer_id),
    FOREIGN KEY (restaurant_id) REFERENCES Restaurants_table(restaurant_id)
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS Delivery_Person_table (
    delivery_person_id VARCHAR(100) PRIMARY KEY,
    name VARCHAR(50),
    contact_number VARCHAR(15),
    vehicle_type VARCHAR(5),
    total_deliveries INT,
    average_rating DOUBLE,
    location VARCHAR(150)
)
""")

c.execute("""
CREATE TABLE IF NOT EXISTS Deliveries_table (
    delivery_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100),
    delivery_person_id VARCHAR(100),
    delivery_status VARCHAR(10),
    distance INT,
    delivery_time INT,
    estimated_time INT,
    delivery_fee DOUBLE,
    vehicle_type VARCHAR(5),
    FOREIGN KEY (order_id) REFERENCES Orders_table(order_id),
    FOREIGN KEY (delivery_person_id) REFERENCES Delivery_Person_table(delivery_person_id)
)
""")

db.commit()

engine = create_engine('mysql+pymysql://root:12345678@localhost/guvidb')

for filename, table in zip(['Customers.csv', 'Restaurants.csv', 'Orders.csv', 'Delivery_Person.csv', 'Deliveries.csv'],
                           ['Customers_table', 'Restaurants_table', 'Orders_table', 'Delivery_Person_table', 'Deliveries_table']):
    df = pd.read_csv(filename)
    df['location'] = df['location'].str[:150]
    print(df['location'].head())
    df.to_sql(table, con=engine, if_exists='append', index=False)
    print(f"{table.replace('_', ' ').title()} data inserted successfully!")

engine = create_engine('mysql+pymysql://root:12345678@localhost/guvidb')
df = pd.read_csv('Customers.csv')
df.to_sql('customers_table', con=engine, if_exists='append', index=False)
print("Customers_table data inserted successfully!")

engine = create_engine('mysql+pymysql://root:12345678@localhost/guvidb')
df = pd.read_csv('Restaurants.csv')
df.to_sql('restaurants_table', con=engine, if_exists='append', index=False)
print("Restaurants_table data inserted successfully!")

engine = create_engine('mysql+pymysql://root:12345678@localhost/guvidb')
df = pd.read_csv('Orders.csv')
df.to_sql('orders_table', con=engine, if_exists='append', index=False)
print("Orders_table data inserted successfully!")

engine = create_engine('mysql+pymysql://root:12345678@localhost/guvidb')
df = pd.read_csv( 'Delivery_Person.csv')
df.to_sql('delivery_person_table', con=engine, if_exists='append', index=False)
print("Delivery_Person_table data inserted successfully!")

engine = create_engine('mysql+pymysql://root:12345678@localhost/guvidb')
df = pd.read_csv('Deliveries.csv')
df.to_sql('deliveries_table', con=engine, if_exists='append', index=False)
print("Deliveries_table data inserted successfully!")
'''

import random
import numpy as np
import pandas as pd
from faker import Faker
from customers import customers_df
from restaurants import restaurants_df

f = Faker('en_IN')

def generate_orders(n, customers, restaurants):
    orders = []

    for _ in range(n):
        customer = random.choice(customers.to_dict('records'))
        restaurant = random.choice(restaurants.to_dict('records'))
        order_datetime = f.date_time_this_month(before_now=False, after_now=True)
        delivery_datetime = order_datetime + pd.Timedelta(minutes=random.randint(10, 180))

        orders.append({
            'order_id': f.unique.uuid4(),
            'customer_id': customer['customer_id'],
            'restaurant_id': restaurant['restaurant_id'],
            'order_date': order_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'delivery_time': delivery_datetime.strftime('%Y-%m-%d %H:%M:%S'),
            'status': random.choice(['Pending', 'Delivered', 'Cancelled']),
            'total_amount': round(random.uniform(50, 10000), 2),
            'payment_mode': random.choice(['Credit Card', 'Cash', 'UPI']),
            'discount_applied': np.random.randint(0, 30),
            'feedback_rating': round(random.uniform(1, 5), 1)
        })

    return pd.DataFrame(orders)
n = 400
orders_df = generate_orders(n, customers_df, restaurants_df)
orders_df = orders_df.drop_duplicates(subset=['order_id'])
orders_df.to_csv('Orders.csv', index=False)
print(orders_df.head())
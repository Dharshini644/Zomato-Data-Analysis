import random
import pandas as pd
import numpy as np
from faker import Faker

from customers import customers_df
from restaurants import restaurants_df
from delivery_person import delivery_person_df
from orders import orders_df

f = Faker('en_IN')

orders_df = orders_df.dropna(subset=['order_id'])

def generate_deliveries(n, customers, restaurants, delivery_person, orders):
    deliveries = []
    for _ in range(n):
        restaurant = random.choice(restaurants.to_dict('records'))
        matching_persons = [p for p in delivery_person.to_dict('records') if p['location'] == restaurant['location']]
        if not matching_persons:
            continue
        person = random.choice(matching_persons)
        order = random.choice(orders.to_dict('records'))
        if pd.isna(order['order_id']):
            continue
        deliveries.append({
            'delivery_id': f.unique.uuid4(),
            'order_id': order['order_id'],
            'delivery_person_id': person['delivery_person_id'],
            'delivery_status': random.choice(['On the way', 'Delivered']),
            'distance': np.random.randint(1, 10),
            'delivery_time': np.random.randint(10, 90),
            'estimated_time': np.random.randint(20, 90),
            'delivery_fee': np.random.randint(20, 100),
            'vehicle_type': person['vehicle_type']
        })
    return pd.DataFrame(deliveries)

deliveries_df = generate_deliveries(len(orders_df), customers_df, restaurants_df, delivery_person_df, orders_df)
deliveries_df = deliveries_df.drop_duplicates(subset=['delivery_id'])
deliveries_df.to_csv('Deliveries.csv', index=False)
print(deliveries_df.head())
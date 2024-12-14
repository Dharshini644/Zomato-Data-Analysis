import random
import pandas as pd
import numpy as np
from faker import Faker

from customers import customers_df
from restaurants import restaurants_df

f = Faker('en_IN')

def generate_delivery_persons(n, customers, restaurants):
    delivery_person = []
    for _ in range(n):
        delivery_person.append({
            'delivery_person_id': f.unique.uuid4(),
            'name': f.name(),
            'contact_number': f.phone_number(),
            'vehicle_type': random.choice(['Bike', 'Car']),
            'total_deliveries': np.random.randint(1, 500),
            'average_rating': round(random.uniform(3, 5), 2),
            'location': random.choice(restaurants['location'])
        })
    return pd.DataFrame(delivery_person)

delivery_person_df = generate_delivery_persons(75, customers_df, restaurants_df)
delivery_person_df = delivery_person_df.drop_duplicates(subset=['delivery_person_id'])
delivery_person_df.to_csv('Delivery_Person.csv', index = False)
print(delivery_person_df.head())
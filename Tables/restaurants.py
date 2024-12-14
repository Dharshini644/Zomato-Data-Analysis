import random
import pandas as pd
from faker import Faker

f = Faker('en_IN')

def generate_restaurants(n):
    restaurants = []
    for _ in range(n):
        restaurants.append({
            'restaurant_id': f.unique.uuid4(),
            'name': f.company() + " Restaurant",
            'cuisine_type': random.choice(['Indian', 'Chinese', 'Italian', 'Mexican', 'Asian', 'Indo-Chinese']),
            'location': f.city(),
            'owner_name': f.name(),
            'average_delivery_time': random.randint(15, 65),
            'contact_number': f.phone_number(),
            'rating': round(random.uniform(1, 5), 1),
            'total_orders': random.randint(1, 100),
            'is_active': random.choice([True, False])
        })
    return pd.DataFrame(restaurants)

restaurants_df = generate_restaurants(80)
restaurants_df = restaurants_df.drop_duplicates(subset=['restaurant_id'])
print(restaurants_df.head())
restaurants_df.to_csv('Restaurants.csv', index = False)
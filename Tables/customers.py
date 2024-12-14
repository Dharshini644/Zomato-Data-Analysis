import random
import pandas as pd
from faker import Faker

f = Faker('en_IN')

def generate_customers(n):
    customers = []
    for _ in range(n):
        name = f.name()
        customers.append({
            'customer_id': f.unique.uuid4(),
            'name': name,
            'email': name.lower().replace(" ", "_") + "@gmail.com",
            'phone': f.phone_number(),
            'location': f.city(),
            'signup_date': f.date_this_decade(),
            'is_premium': random.choice([True, False]),
            'preferred_cuisine': random.choice(['Indian', 'Chinese', 'Italian', 'Mexican', 'Asian', 'Indo-Chinese']),
            'total_orders': random.randint(1, 100),
            'average_rating': round(random.uniform(3, 5), 2)
        })
    return pd.DataFrame(customers)

customers_df = generate_customers(250)
customers_df = customers_df.drop_duplicates(subset=['customer_id'])
print(customers_df.head())
customers_df.to_csv('Customers.csv',index = False)
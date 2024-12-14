import mysql.connector
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os

def get_db_connection():
    try:
        db = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            user=os.getenv("DB_USER", "root"),
            passwd=os.getenv("DB_PASS", "12345678"),
            database=os.getenv("DB_NAME", "guvidb")
        )
        return db
    except mysql.connector.Error as err:
        st.error(f"Error connecting to database: {err}")
        return None

def get_table_names():
    db = get_db_connection()
    if db:
        try:
            c = db.cursor()
            c.execute("SHOW TABLES")
            return [row[0] for row in c.fetchall()]
        except mysql.connector.Error as err:
            st.error(f"Error fetching tables: {err}")
            return []
        finally:
            db.close()
    return []

def create_table(table_name, columns):
    db = get_db_connection()
    if db:
        try:
            c = db.cursor()
            columns_def = ', '.join([f"{col['name']} {col['type']}" for col in columns])
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"
            c.execute(query)
            db.commit()
            st.success(f"Table `{table_name}` created successfully!")
        except mysql.connector.Error as err:
            st.error(f"Error creating table: {err}")
        finally:
            db.close()

def read_table(table_name):
    engine = create_engine('mysql+pymysql://root:12345678@localhost/guvidb')
    try:
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, con=engine)
    except SQLAlchemyError as err:
        st.error(f"Error reading table: {err}")
        return pd.DataFrame()

def update_table(table_name, updates, condition):
    db = get_db_connection()
    if db:
        try:
            c = db.cursor()
            set_clause = ', '.join([f"{col} = %s" for col in updates.keys()])
            query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
            c.execute(query, list(updates.values()))
            db.commit()
            st.success(f"Record(s) updated in `{table_name}`!")
        except mysql.connector.Error as err:
            st.error(f"Error updating table: {err}")
        finally:
            db.close()

def delete_from_table(table_name, condition):
    db = get_db_connection()
    if db:
        try:
            c = db.cursor()
            query = f"DELETE FROM {table_name} WHERE {condition}"
            c.execute(query)
            db.commit()
            st.success(f"Record(s) deleted from `{table_name}`!")
        except mysql.connector.Error as err:
            st.error(f"Error deleting records: {err}")
        finally:
            db.close()

def add_values(table_name):
    st.subheader(f"Add New Record to {table_name}")
    df = read_table(table_name)
    if df.empty:
        st.warning("The table is empty or does not exist.")
        return

    with st.form(key=f"add_record_form_{table_name}"):
        inputs = {}
        for column in df.columns:
            inputs[column] = st.text_input(f"Enter value for {column}")

        submitted = st.form_submit_button("Add Record")
        if submitted:
            db = get_db_connection()
            if db:
                try:
                    c = db.cursor()
                    columns = ', '.join(inputs.keys())
                    placeholders = ', '.join(['%s'] * len(inputs))
                    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    c.execute(query, list(inputs.values()))
                    db.commit()
                    st.success(f"New record added to `{table_name}`!")
                except mysql.connector.Error as err:
                    st.error(f"Error adding record: {err}")
                finally:
                    db.close()

def perform_data_analysis(selected_table):
    st.subheader("Data Analysis")
    analysis_category = st.selectbox(
        "Choose an Analysis Category",
        ["None", "Order Management", "Customer Analytics", "Delivery Optimization", "Restaurant Insights", "Delivery Performance"],
        key="analysis_category"
    )
    
    df = read_table(selected_table)
    
    required_columns = {
        "Order Management": ["order_date", "status"],
        "Customer Analytics": ["preferred_cuisine", "customer_id", "total_orders"],
        "Delivery Optimization": ["delivery_time", "delivery_person_id"],
        "Delivery Performance": ["delivery_person_id", "average_rating"],
        "Restaurant Insights": ["name", "total_orders"]
    }
    
    missing_columns = [col for col in required_columns.get(analysis_category, []) if col not in df.columns]
    
    if missing_columns:
        st.error(f"The selected table does not contain the required columns: {', '.join(missing_columns)}")
        return

    if analysis_category == "Order Management":
        st.write("### Order Management Analysis")
        if st.checkbox("Identify Peak Ordering Times"):
            df["order_date"] = pd.to_datetime(df["order_date"])
            peak_hours = df["order_date"].dt.hour.value_counts().sort_index()
            st.bar_chart(peak_hours)

        if st.checkbox("Track Delayed and Canceled Deliveries"):
            status_counts = df["status"].value_counts()
            st.bar_chart(status_counts)

    elif analysis_category == "Customer Analytics":
        st.write("### Customer Analytics")
        if st.checkbox("Analyze Customer Preferences"):
            preferences = df["preferred_cuisine"].value_counts()
            st.bar_chart(preferences)

        if st.checkbox("Identify Top Customers"):
            top_customers = df.groupby("customer_id")["total_orders"].sum().nlargest(10)
            st.bar_chart(top_customers)

    elif analysis_category == "Delivery Optimization":
        st.write("### Delivery Optimization")
        if st.checkbox("Analyze Delivery Times"):
            st.line_chart(df["delivery_time"])

    elif analysis_category == "Delivery Performance":
        st.write("### Delivery Performance")
        if st.checkbox("Track Delivery Personnel Performance"):
            performance = df.groupby("delivery_person_id")["average_rating"].mean()
            st.bar_chart(performance)

    elif analysis_category == "Restaurant Insights":
        st.write("### Restaurant Insights")
        popular_restaurants = df.groupby("name")["total_orders"].count().sort_values(ascending=False).head(10)
        st.bar_chart(popular_restaurants)

        order_values = df.groupby("name")["total_orders"].sum().sort_values(ascending=False).head(10)
        st.bar_chart(order_values)

def main():
    st.title("Zomato Data Management Tool")

    menu = ["Home", "CRUD Operations", "Data Analysis"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Home":
        st.subheader("Welcome to the Zomato Data Management Tool")
        st.subheader("Project overview")
        st.text("""
        The Zomato Data Management Tool is a comprehensive data management and analytics platform built with Streamlit, MySQL, and Python. This application is designed for managing, analyzing, and visualizing data in a structured and efficient manner. It is tailored for use cases like order management, customer analytics, delivery optimization, and restaurant insights, making it ideal for food delivery businesses or similar industries.
        *Key Features*
        1. CRUD Operations
        The tool offers a full suite of CRUD (Create, Read, Update, Delete) operations for managing database tables:

        Create Tables: Users can create new database tables dynamically by specifying the table name and column details.
        Read Data: Fetch and view table data directly in the Streamlit app with support for dynamic table selection.
        Update Records: Update existing table records by specifying column-value pairs and a condition.
        Delete Records: Remove records from a table by providing a condition.
        Add Records: Add new rows of data to a selected table through an intuitive form interface.
        2. Data Analysis
        The tool provides domain-specific analysis features to derive insights from the data:
        Order Management: Analyze peak ordering times, track delayed or canceled deliveries.
        Customer Analytics: Study customer preferences and identify top customers.
        Delivery Optimization: Evaluate delivery times and suggest improvements.
        Delivery Performance: Track delivery personnel performance based on ratings.
        Restaurant Insights: Analyze restaurant popularity and order values.
""")

    elif choice == "CRUD Operations":
        st.subheader("CRUD Operations")
        operation = st.selectbox("Select an operation", ["Create", "Read", "Update", "Delete", "Add Values"])

        if operation == "Create":
            table_name = st.text_input("Table Name")
            columns = []
            col_count = st.number_input("Number of columns", min_value=1, max_value=10, step=1)

            for i in range(col_count):
                col_name = st.text_input(f"Column {i+1} Name")
                col_type = st.selectbox(f"Column {i+1} Type", ["VARCHAR(255)", "INT", "FLOAT", "TEXT"], key=f"type_{i}")
                columns.append({"name": col_name, "type": col_type})

            if st.button("Create Table"):
                create_table(table_name, columns)

        elif operation == "Read":
            table_names = get_table_names()
            table_name = st.selectbox("Select Table", table_names)
            if table_name and st.button("Fetch Data"):
                df = read_table(table_name)
                st.write(df)

        elif operation == "Update":
            table_names = get_table_names()
            table_name = st.selectbox("Select Table", table_names)
            updates = st.text_area("Updates (e.g., column=value, column=value,...)")
            condition = st.text_input("Condition (e.g., id=1)")

            if st.button("Update Record"):
                updates_dict = {item.split('=')[0].strip(): item.split('=')[1].strip() for item in updates.split(',')}
                update_table(table_name, updates_dict, condition)

        elif operation == "Delete":
            table_names = get_table_names()
            table_name = st.selectbox("Select Table", table_names)
            condition = st.text_input("Condition (e.g., id=1)")

            if st.button("Delete Record"):
                delete_from_table(table_name, condition)

        elif operation == "Add Values":
            table_names = get_table_names()
            table_name = st.selectbox("Select Table", table_names)
            if table_name:
                add_values(table_name)

    elif choice == "Data Analysis":
        table_names = get_table_names()  # Ensure table_names is defined
        if table_names:
            selected_table = st.sidebar.selectbox("Select a table for analysis", table_names)
            if selected_table:
                perform_data_analysis(selected_table)
        else:
            st.warning("No tables available for analysis. Please create tables first.")

if __name__ == "__main__":
    main()

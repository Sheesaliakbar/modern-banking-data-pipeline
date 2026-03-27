import random
from faker import Faker
import psycopg2
from decimal import Decimal
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv


load_dotenv()

# -----------------------------
# DATABASE CONNECTION
# -----------------------------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"), 
    database=os.getenv("POSTGRES_DATABASE"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    port=os.getenv("POSTGRES_PORT"),
    )
cursor = conn.cursor()

# -----------------------------
# FAKER SETUP (PAKISTAN)
# -----------------------------
fake = Faker("en_PK")

# -----------------------------
# CONFIG
# -----------------------------
NUM_CUSTOMERS = 50
NUM_BRANCHES = 5
NUM_EMPLOYEES = 20
NUM_ACCOUNTS = 70
NUM_TRANSACTIONS = 200
NUM_LOANS = 20

# -----------------------------
# INSERT BRANCHES
# -----------------------------
def insert_branches():
    branches = []
    cities = ["Karachi", "Lahore", "Islamabad", "Rawalpindi", "Faisalabad"]

    for i in range(NUM_BRANCHES):
        branches.append((
            f"{cities[i]} Main Branch"[:50],
            fake.address()[:100],
            cities[i][:50],
            fake.postcode()[:10]
        ))

    query = """
        INSERT INTO branches (branch_name, address, city, zip_code)
        VALUES (%s, %s, %s, %s)
        RETURNING branch_id
    """
    execute_batch(cursor, query, branches)
    conn.commit()
    print("✅ Branches inserted")

# -----------------------------
# INSERT CUSTOMERS
# -----------------------------
def insert_customers():
    customers = []

    for _ in range(NUM_CUSTOMERS):
        customers.append((
            fake.first_name()[:20],
            fake.last_name()[:20],
            fake.unique.email()[:50],
            fake.phone_number()[:20],
            fake.date_of_birth(minimum_age=18, maximum_age=65)
        ))

    query = """
        INSERT INTO customers
        (first_name, last_name, email, phone, date_of_birth)
        VALUES (%s, %s, %s, %s, %s)
    """
    execute_batch(cursor, query, customers)
    conn.commit()
    print("✅ Customers inserted")

# -----------------------------
# INSERT EMPLOYEES
# -----------------------------
def insert_employees():
    cursor.execute("SELECT branch_id FROM branches")
    branch_ids = [row[0] for row in cursor.fetchall()]

    employees = []
    positions = ["Manager", "Cashier", "Officer", "Clerk"]

    for _ in range(NUM_EMPLOYEES):
        employees.append((
            fake.first_name()[:20],
            fake.last_name()[:20],
            fake.unique.email()[:50],
            fake.phone_number()[:20],
            random.choice(branch_ids),
            random.choice(positions)[:20]
        ))

    query = """
        INSERT INTO employees
        (first_name, last_name, email, phone, branch_id, position)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    execute_batch(cursor, query, employees)
    conn.commit()
    print("✅ Employees inserted")

# -----------------------------
# INSERT ACCOUNTS
# -----------------------------
def insert_accounts():
    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT branch_id FROM branches")
    branch_ids = [row[0] for row in cursor.fetchall()]

    accounts = []
    account_types = ["savings", "checking"]

    for _ in range(NUM_ACCOUNTS):
        accounts.append((
            fake.unique.bban()[:20],
            random.choice(customer_ids),
            random.choice(branch_ids),
            random.choice(account_types),
            Decimal(round(random.uniform(1000, 500000), 2)),
            "active"
        ))

    query = """
        INSERT INTO accounts
        (account_number, customer_id, branch_id, account_type, balance, status)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    execute_batch(cursor, query, accounts)
    conn.commit()
    print("✅ Accounts inserted")

# -----------------------------
# INSERT TRANSACTIONS
# -----------------------------
def insert_transactions():
    cursor.execute("SELECT account_id, balance FROM accounts")
    accounts_list = cursor.fetchall()

    transactions = []
    transaction_types = ["deposit", "withdrawal"]

    for _ in range(NUM_TRANSACTIONS):
        account_id, balance = random.choice(accounts_list)
        balance = Decimal(balance)
        amount = Decimal(round(random.uniform(500, 20000), 2))
        t_type = random.choice(transaction_types)

        if t_type == "withdrawal":
            amount = min(amount, balance)

        balance_after = balance + amount if t_type == "deposit" else balance - amount

        transactions.append((
            account_id,
            t_type,
            amount,
            fake.sentence()[:100],
            balance_after
        ))

    query = """
        INSERT INTO transactions
        (account_id, transaction_type, amount, description, balance_after)
        VALUES (%s, %s, %s, %s, %s)
    """
    execute_batch(cursor, query, transactions)
    conn.commit()
    print("✅ Transactions inserted")

# -----------------------------
# INSERT LOANS
# -----------------------------
def insert_loans():
    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT branch_id FROM branches")
    branch_ids = [row[0] for row in cursor.fetchall()]

    loans = []
    loan_types = ["personal", "auto", "mortgage"]

    for _ in range(NUM_LOANS):
        start_date = fake.date_between(start_date="-5y", end_date="today")
        end_date = start_date + timedelta(days=random.randint(365, 3650))

        loans.append((
            random.choice(customer_ids),
            random.choice(branch_ids),
            random.choice(loan_types),
            Decimal(round(random.uniform(100000, 5000000), 2)),
            Decimal(round(random.uniform(5, 20), 2)),
            start_date,
            end_date,
            "active"
        ))

    query = """
        INSERT INTO loans
        (customer_id, branch_id, loan_type, principal_amount,
         interest_rate, start_date, end_date, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    execute_batch(cursor, query, loans)
    conn.commit()
    print("✅ Loans inserted")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    insert_branches()
    insert_customers()
    insert_employees()
    insert_accounts()
    insert_transactions()
    insert_loans()

    cursor.close()
    conn.close()
    print("\n Banking data generated successfully!")

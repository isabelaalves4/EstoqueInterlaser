import os
import psycopg

def get_connection():
    url = os.getenv("DATABASE_URL")
    return psycopg.connect(url)


# Compatibilidade com versões antigas do app.py
get_db_connection = get_connection

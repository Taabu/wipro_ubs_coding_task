import sqlite3
import time

class DatabaseHandler:
    def __init__(self, database_path):
        self.database_path = database_path
        self.connection = None
        self.last_modification_time = 0
        self.modifiers_cache = []

    def connect(self):
        self.connection = sqlite3.connect(self.database_path)

    def create_database(self):
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS INSTRUMENT_PRICE_MODIFIER (
                    ID INTEGER PRIMARY KEY AUTOINCREMENT,
                    NAME TEXT NOT NULL,
                    MULTIPLIER REAL NOT NULL,
                    UNIQUE (NAME)
                )
            ''')
            self.connection.commit()

    def add_or_update_multiplier(self, instrument_name, multiplier):
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO INSTRUMENT_PRICE_MODIFIER (NAME, MULTIPLIER)
                VALUES (?, ?)
            ''', (instrument_name, multiplier))
            self.connection.commit()

            # Invalidate the cache after an update
            self.modifiers_cache = []

    def fetch_instrument_modifiers(self):
        current_time = time.time()

        # Check if more than 5 seconds have passed since the last modification or if the cache is empty
        if current_time - self.last_modification_time >= 5 or not self.modifiers_cache:
            query = "SELECT NAME as instrument, MULTIPLIER FROM INSTRUMENT_PRICE_MODIFIER"
            modifiers_data = self.connection.execute(query).fetchall()
            self.last_modification_time = current_time
            self.modifiers_cache = modifiers_data
            return modifiers_data
        else:
            # Return data from the cache if less than 5 seconds have passed and the cache is not empty
            return self.modifiers_cache

    def close_connection(self):
        if self.connection:
            self.connection.close()
            self.connection = None

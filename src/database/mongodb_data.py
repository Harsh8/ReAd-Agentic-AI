import os
import pandas as pd
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import dotenv
dotenv.load_dotenv()

class Mongodatabase:
    def __init__(self, username : str, password: str, db_name: str):
        self.username = username
        self.password = password
        self.db_name = db_name
        self.uri = f"mongodb+srv://{username}:{password}@{db_name}.ayo4j9b.mongodb.net/?appName=Cluster-ML-Data"
        self.db = None

    def connect(self):
        """Establish a connection to the MongoDB server."""
        try:
            self.client = MongoClient(self.uri)
            self.client.admin.command('ping')
            print("Successfully connected to MongoDB")
            self.db = self.client[self.db_name]
        except ConnectionFailure as e:
            print(f"Error connecting to MongoDB: {e}")

    def upload_data(self, collection_name: str, df: pd.DataFrame):
            if self.db is None:
                print("No database connection. Please connect to the database first.")
                return
            
            collection = self.db[collection_name]
            data = df.to_dict(orient='records')
            collection.insert_many(data)
            print(f"Data uploaded to collection '{collection_name}' successfully.")
    
    def download_data(self, collection_name: str) -> pd.DataFrame:
            if self.db is None:
                print("No database connection. Please connect to the database first.")
                return pd.DataFrame()
            
            collection = self.db[collection_name]
            data = list(collection.find())
            df = pd.DataFrame(data)
            print(f"Data downloaded from collection '{collection_name}' successfully.")
            return df

    def close_connection(self):
        """Close the connection to the MongoDB server."""
        if self.client:
            self.client.close()
            print("MongoDB connection closed.")     

if __name__ == "__main__":
     
     patient_csv = '/Users/harshprajapati/Desktop/ReAd_AgenticAI/data/readmission_training_data.csv'
     patient_df = pd.read_csv(patient_csv) 
    
     mongo_username = os.getenv("MONGO_USERNAME")
     mongo_password = os.getenv("MONGO_PASSWORD")
     mongo_db_name = os.getenv("MONGO_DB_NAME")

     patientdb = Mongodatabase(mongo_username, mongo_password, mongo_db_name)
     patientdb.connect()    
     patientdb.upload_data("patients_features", patient_df)
     patientdb.close_connection()
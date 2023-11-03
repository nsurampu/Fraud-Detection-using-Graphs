from neo4j import GraphDatabase
import pandas as pd


class FraudDB:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def execute_query(self, query, returns=False):
        try:
            records, summary, keys = self.driver.execute_query(query)
        except Exception as e:
            print(e)
            records = []
        if returns:
            return [record.data() for record in records]

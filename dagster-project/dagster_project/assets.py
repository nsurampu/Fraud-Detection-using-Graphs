import pandas as pd
from neo4j import GraphDatabase
from pathlib import Path
import os

from dagster import AssetExecutionContext, MetadataValue, asset

from neo4j_connector import FraudDB
from neo4j_queries import Neo4JQueries

queries_obj = Neo4JQueries()
queries_obj.populate_queries()
queries = queries_obj.queries

@asset
def calculate_first_party_fraud():
    connector = FraudDB("bolt://localhost:7687", "neo4j", "password")
    connector.execute_query(queries['link_shared_identifier'])
    connector.execute_query(queries['create_base_graph'])
    connector.execute_query(queries['identify_fraud_rings'])
    connector.execute_query(queries['write_fraud_rings_to_db'])
    connector.execute_query(queries['compute_fraud_ring_similarity'])
    connector.execute_query(queries['write_fraud_ring_similarity_to_db'])
    connector.execute_query(queries['write_fraud_ring_similarity_to_graph'])
    connector.execute_query(queries['stamp_fraudster'])
    first_party_result = connector.execute_query(queries['get_fraudster'], returns=True)
    txn_result = connector.execute_query(queries['get_fraudster_transactions'], returns=True)
    connector.close()
    result = pd.DataFrame(columns=['Client ID', 'Fraud Score'])
    result['Client ID'] = [x['Client_ID'] for x in first_party_result]
    result['Fraud Score'] = [x['Fraud_Score'] for x in first_party_result]
    result.drop_duplicates(subset=['Client ID'], inplace=True)
    if(Path('data/first_party_fraud.xlsx').is_file()):
        if(Path('data/first_party_fraud_old.xlsx').is_file()):
            os.remove('data/first_party_fraud_old.xlsx')
        os.rename('data/first_party_fraud.xlsx', 'data/first_party_fraud_old.xlsx')
    result.to_excel('data/first_party_fraud.xlsx', index=False)
    transaction_result = pd.DataFrame(columns=['Client ID', 'Secondary Client ID', 'Transaction ID', 'Transaction Amount'])
    transaction_result['Client ID'] = [x['Client_ID'] for x in txn_result]
    transaction_result['Secondary Client ID'] = [x['Secondary_Client_ID'] for x in txn_result]
    transaction_result['Transaction ID'] = [x['Transaction_ID'] for x in txn_result]
    transaction_result['Transaction Amount'] = [x['Transaction_Amount'] for x in txn_result]
    transaction_result.drop_duplicates(subset=['Client ID', 'Secondary Client ID', 'Transaction ID'], inplace=True)
    if(Path('data/first_party_fraud_transactions.xlsx').is_file()):
        if(Path('data/first_party_fraud_transactions_old.xlsx').is_file()):
            os.remove('data/first_party_fraud_transactions_old.xlsx')
        os.rename('data/first_party_fraud_transactions.xlsx', 'data/first_party_fraud_transactions_old.xlsx')
    transaction_result.to_excel('data/first_party_fraud_transactions.xlsx', index=False)

@asset(deps=[calculate_first_party_fraud])
def calculate_second_party_fraud():
    connector = FraudDB("bolt://localhost:7687", "neo4j", "password")
    connector.execute_query(queries['add_second_party_tag'])
    connector.execute_query(queries['add_transfer_to_relation'])
    connector.execute_query(queries['create_transfer_to_projection'])
    connector.execute_query(queries['create_second_party_clusters'])
    connector.execute_query(queries['second_party_page_rank'])
    mules_result = connector.execute_query(queries['get_mules'], returns=True)
    result = pd.DataFrame(columns=["First Party Fraud Client ID", "First Party Fraud Score", "Second Party Fraud Client ID", "Second Party Fraud Score"])
    result['First Party Fraud Client ID'] = [x['p'][0]['id'] for x in mules_result]
    result['First Party Fraud Score'] = [x['p'][0]['firstPartyFraudScore'] for x in mules_result]
    result['Second Party Fraud Client ID'] = [x['p'][2]['id'] for x in mules_result]
    result['Second Party Fraud Score'] = [x['p'][2]['secondPartyFraudScore'] for x in mules_result]
    result.drop_duplicates(subset=['First Party Fraud Client ID', 'Second Party Fraud Client ID'], inplace=True)
    result.sort_values(by=['First Party Fraud Score', 'Second Party Fraud Score'], ascending=False, inplace=True)
    if(Path('data/second_party_fraud.xlsx').is_file()):
        if(Path('data/second_party_fraud_old.xlsx').is_file()):
            os.remove('data/second_party_fraud_old.xlsx')
        os.rename('data/second_party_fraud.xlsx', 'data/second_party_fraud_old.xlsx')
    result.to_excel('data/second_party_fraud.xlsx', index=False)
    connector.close()

@asset(deps=[calculate_second_party_fraud])
def clear_graphs():
    connector = FraudDB("bolt://localhost:7687", "neo4j", "password")
    connector.execute_query(queries['clear_graphs'])
    connector.close()

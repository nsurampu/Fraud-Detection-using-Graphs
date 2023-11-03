import streamlit as st

import time
from datetime import datetime

import pandas as pd
import numpy as np

st.set_page_config(page_title="Transactions (Demo)", layout="wide")

def load_data(data_path):
    data = pd.read_excel(data_path)
    return data


if __name__=="__main__":

    data_load_state = st.text("Loading data...")
    first_party_fraud_txn = load_data('dagster-project/data/first_party_fraud_transactions.xlsx')
    first_party_fraud_txn['Client ID'] = first_party_fraud_txn['Client ID'].apply(lambda x: str(x))
    first_party_fraud_txn['Secondary Client ID'] = first_party_fraud_txn['Secondary Client ID'].apply(lambda x: str(x))
    first_party_fraud_txn['Transaction Amount'] = first_party_fraud_txn["Transaction Amount"].apply(lambda x: int(x))
    first_party_fraud_txn.rename(columns={'Transaction Amount': 'Transaction Amount ($)'}, inplace=True)
    data_load_state.text('Loading data...done!')

    st.write("# Transactions")
    
    st.subheader("List of Transactions flagged under First Party Fraud Clients")
    
    choice1 = st.text_input("Primary Client ID", "")
    choice2 = st.text_input("Secondary Client ID", "")

    if choice1=="" and choice2=="":
        st.table(first_party_fraud_txn)
    elif choice1!="" and choice2=="":
        st.table(first_party_fraud_txn.loc[(first_party_fraud_txn['Client ID']==choice1)])
    elif choice1=="" and choice2!="":
        st.table(first_party_fraud_txn.loc[(first_party_fraud_txn['Secondary Client ID']==choice2)])
    else:
        st.table(first_party_fraud_txn.loc[(first_party_fraud_txn['Client ID']==choice1) & (first_party_fraud_txn['Secondary Client ID']==choice2)])
    
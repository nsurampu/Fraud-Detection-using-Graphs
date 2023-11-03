import streamlit as st

import time
from datetime import datetime

import pandas as pd
import numpy as np

st.set_page_config(page_title="Primary Party Fraud (Demo)", layout="wide")

def load_data(data_path):
    data = pd.read_excel(data_path)
    return data

def process_data(data1, data2):
    data1['Number of Transactions'] = data1['Client ID'].apply(lambda x: data2.loc[data2['Client ID']==x].shape[0])
    data1['Total Transactions Amount ($)'] = data1['Client ID'].apply(lambda x: data2.loc[data2['Client ID']==x]['Transaction Amount'].sum())
    data1['Total Transactions Amount ($)'] = data1['Total Transactions Amount ($)'].apply(lambda x: int(x))
    return data1

if __name__=="__main__":

    data_load_state = st.text("Loading data...")
    first_party_fraud = load_data('dagster-project/data/first_party_fraud.xlsx')
    first_party_fraud_old = load_data('dagster-project/data/first_party_fraud_old.xlsx')
    first_party_fraud_txn = load_data('dagster-project/data/first_party_fraud_transactions.xlsx')
    first_party_fraud_txn_old = load_data('dagster-project/data/first_party_fraud_transactions_old.xlsx')
    
    first_party_fraud['Client ID'] = first_party_fraud['Client ID'].apply(lambda x: str(x))
    first_party_fraud_old['Client ID'] = first_party_fraud_old['Client ID'].apply(lambda x: str(x))
    first_party_fraud_txn['Client ID'] = first_party_fraud_txn['Client ID'].apply(lambda x: str(x))
    first_party_fraud_txn_old['Client ID'] = first_party_fraud_txn_old['Client ID'].apply(lambda x: str(x))

    first_party_fraud = process_data(first_party_fraud, first_party_fraud_txn)
    first_party_fraud_old = process_data(first_party_fraud_old, first_party_fraud_txn_old)

    first_party_fraud.drop(['Fraud Score'], axis=1, inplace=True)
    
    data_load_state.text('Loading data...done!')

    st.write("# Primary Party Fraud Detection")

    # st.sidebar.header("First Party Fraud Detection")
    
    # st.subheader('Clients flagged under Primary Party Fraud')

    col1, col2, col3 = st.columns(3)
    col1.metric("Number of Flagged Clients", str(first_party_fraud.shape[0]),
    str(round((first_party_fraud.shape[0]-first_party_fraud_old.shape[0] * 1.03286) / first_party_fraud.shape[0] * 100, 1)) + "%", delta_color="inverse")
    col2.metric("Number of Transactions by Flagged Clients", str(first_party_fraud['Number of Transactions'].sum()),
    str(round((first_party_fraud['Number of Transactions'].sum()-first_party_fraud_old['Number of Transactions'].sum() * 0.83286) / first_party_fraud['Number of Transactions'].sum() * 100, 1)) + "%", delta_color="inverse")
    col3.metric("Total Transacted Amount by Flagged Clients", "$" + "{:,}".format(first_party_fraud["Total Transactions Amount ($)"].sum()),
    "{:,}".format(round((first_party_fraud["Total Transactions Amount ($)"].sum()-first_party_fraud_old["Total Transactions Amount ($)"].sum() * 0.93286) / first_party_fraud["Total Transactions Amount ($)"].sum() * 100, 1)) + "%", delta_color="inverse")

    top = st.slider("Display Records", 1, first_party_fraud.shape[0], 5)
    st.table(first_party_fraud.iloc[:top, :])

    st.bar_chart(first_party_fraud, x="Client ID", y="Number of Transactions")

    st.area_chart(first_party_fraud, x='Client ID', y="Total Transactions Amount ($)", color="Number of Transactions")

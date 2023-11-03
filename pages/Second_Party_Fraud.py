import streamlit as st

import time
from datetime import datetime

import pandas as pd
import numpy as np

st.set_page_config(page_title="Second Party Fraud (Demo)", layout="wide")

def load_data(data_path):
    data = pd.read_excel(data_path)
    return data


if __name__=="__main__":

    data_load_state = st.text("Loading data...")
    second_party_fraud = load_data('dagster-project/data/second_party_fraud.xlsx')
    first_party_fraud_txn = load_data('dagster-project/data/first_party_fraud_transactions.xlsx')
    second_party_fraud['First Party Fraud Client ID'] = second_party_fraud['First Party Fraud Client ID'].apply(lambda x: str(x))
    second_party_fraud['Second Party Fraud Client ID'] = second_party_fraud['Second Party Fraud Client ID'].apply(lambda x: str(x))
    first_party_fraud_txn['Client ID'] = first_party_fraud_txn['Client ID'].apply(lambda x: str(x))
    first_party_fraud_txn['Secondary Client ID'] = first_party_fraud_txn['Secondary Client ID'].apply(lambda x: str(x))
    second_party_fraud['Number of Transactions'] = second_party_fraud[['First Party Fraud Client ID',
    'Second Party Fraud Client ID']].apply(lambda x: first_party_fraud_txn.loc[(first_party_fraud_txn['Client ID']==x[0]) & (first_party_fraud_txn['Secondary Client ID']==x[1])].shape[0], axis=1)
    second_party_fraud['Total Transactions Amount ($)'] = second_party_fraud[['First Party Fraud Client ID',
    'Second Party Fraud Client ID']].apply(lambda x: first_party_fraud_txn.loc[(first_party_fraud_txn['Client ID']==x[0]) & (first_party_fraud_txn['Secondary Client ID']==x[1])]['Transaction Amount'].sum(), axis=1)
    second_party_fraud['Total Transactions Amount ($)'] = second_party_fraud['Total Transactions Amount ($)'].apply(lambda x: int(x))
    data_load_state.text('Loading data...done!')

    second_party_fraud.drop(['First Party Fraud Score'], axis=1, inplace=True)
    second_party_fraud.drop(['Second Party Fraud Score'], axis=1, inplace=True)

    st.write("# Second Party Fraud Detection (Money Mules)")
    
    # st.subheader('Clients flagged under Second Party Fraud')
    
    choice = st.selectbox("First Party Fraud Client ID", set(second_party_fraud['First Party Fraud Client ID'].values))
    top = st.slider("Display Records", 1, 10, 10)
    st.table(second_party_fraud.loc[second_party_fraud['First Party Fraud Client ID']==choice].iloc[:top, :])

    # st.bar_chart(second_party_fraud.loc[second_party_fraud['First Party Fraud Client ID']==choice], x="Second Party Fraud Client ID", y="Number of Transactions")

import streamlit as st

import time
from datetime import datetime

import pandas as pd
import numpy as np

st.set_page_config(page_title="Fraud Detection (Demo)", layout="wide")

def load_data(data_path):
    data = pd.read_excel(data_path)
    return data


if __name__=="__main__":

    # data_load_state = st.text("Loading data...")
    # first_party_fraud = load_data('dagster-project/data/first_party_fraud.xlsx')
    # first_party_fraud['Customer ID'] = first_party_fraud['Customer ID'].apply(lambda x: str(x))
    # data_load_state.text('Loading data...done!')

    # st.sidebar.headr
    
    # st.subheader('Clients flagged under First Party Fraud')
    # st.table(first_party_fraud)

    st.write("# Welcome to Fraud Detection Demo!")

    st.sidebar.success("Select a dashboard to view")

    st.markdown(
        """
        **Problem statement**: Focuses on identifying personal banking clients who are exhibiting suspicious activity. Subsequently, we also use that information for
        identifying "money mules" who might potentially be aiding clients in comitting frauds.

        **Hypothesis**: Clients who share identifiers are suspicious and have a higher potential to commit fraud. However, all shared identifier links are not suspicious, 
        for example, two people sharing an email address. Hence, we compute a fraud score based on shared PII relationships and label the top 0.9 percentile clients as fraudsters.
        """
    )

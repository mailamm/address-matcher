import streamlit as st
import pandas as pd
from sklearn.metrics import accuracy_score
import altair as alt

# Set Streamlit page configuration
st.set_page_config(page_title="Match Accuracy Dashboard", layout="wide")

# Display dashboard title
st.title("Address Matching Accuracy Dashboard")

@st.cache_data
def load_data(matched_path, truth_path):
    """
    Load and merge matched output with ground-truth data on transaction_id.
    Caches result to avoid repeated I/O.
    """
    df_matched = pd.read_csv(matched_path)
    df_truth   = pd.read_csv(truth_path)
    return df_matched.merge(df_truth, on='transaction_id', how='inner')

# Sidebar input for file paths
matched_file = st.sidebar.text_input("Matched CSV path", "matched_output.csv")
truth_file   = st.sidebar.text_input("Ground-truth CSV", "ground_truth.csv")

# Run analysis when button is clicked
if st.sidebar.button("Load and Analyze"):
    # Load merged dataset
    df = load_data(matched_file, truth_file)
    st.write("## Merged Data", df)

    # Replace missing values with placeholder and cast as string
    df['address_id'] = df['address_id'].fillna('__UNMATCHED__').astype(str)
    df['true_address_id'] = df['true_address_id'].fillna('__UNMATCHED__').astype(str)

    # Get predictions and ground-truth
    y_pred = df['address_id']
    y_true = df['true_address_id']

    # Compute and display overall accuracy
    acc = accuracy_score(y_true, y_pred)
    st.metric("Overall Accuracy", f"{acc:.2%}")

    # Filter and display correct matches
    correct = (df['address_id'] == df['true_address_id'])
    st.write("### Correct Matches")
    st.dataframe(df[correct])

    # Filter and display incorrect matches
    st.write("### Incorrect Matches")
    st.dataframe(df[~correct][['transaction_id', 'true_address_id', 'address_id', 'match_type', 'reason']])

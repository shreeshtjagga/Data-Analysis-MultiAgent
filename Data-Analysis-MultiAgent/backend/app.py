import streamlit as st
import pandas as pd

# Page config
st.set_page_config(page_title="Data Analyst", layout="wide")

# Title
st.title("Agentic AI Data Analyst")

# File upload
uploaded_file = st.file_uploader("Upload your dataset (CSV)", type=["csv"])

# Analyze button
if st.button("Analyze"):
    if uploaded_file is not None:
        try:
            # Read CSV
            df = pd.read_csv(uploaded_file)

            # Success message
            st.success("File uploaded successfully")

            # Metrics
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Rows", df.shape[0])
            with col2:
                st.metric("Columns", df.shape[1])

            # Data Preview
            st.subheader("Data Preview")
            st.dataframe(df.head())

            # Placeholder message
            st.info("Analysis pipeline initializing... Insights will be ready soon!")

        except Exception as e:
            st.error(f"Error reading file: {str(e)}")

    else:
        st.warning("Please upload a CSV file first!")
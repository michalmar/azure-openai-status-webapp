import streamlit as st
import pandas as pd
import glob
import os
import io
from dotenv import load_dotenv, find_dotenv
from azure.storage.blob import BlobClient, ContainerClient

AZURE_STORAGE_CONNECTION_STRING = os.getenv('AZURE_STORAGE_CONNECTION_STRING', None)
AZURE_STORAGE_CONTAINER_NAME_IMAGES = os.getenv('AZURE_STORAGE_CONTAINER_NAME_IMAGES', None)
AZURE_STORAGE_CONTAINER_NAME_DOCS = os.getenv('AZURE_STORAGE_CONTAINER_NAME_DOCS', None)

# Load environment variables from the .env file
load_dotenv(find_dotenv())


def load_data_from_blob():
    """
    Fetches all CSV files from a specified Azure Blob Storage container and combines them into a single Pandas DataFrame.

    Parameters:
    - container_name (str): The name of the container from which to fetch CSV files.

    Returns:
    - pd.DataFrame: A DataFrame containing the combined data from all CSV files in the container.
    """
    # Initialize the BlobServiceClient
    container_client = ContainerClient.from_connection_string(conn_str=AZURE_STORAGE_CONNECTION_STRING, container_name=AZURE_STORAGE_CONTAINER_NAME_DOCS)
        

    # List all blobs in the container and filter for CSV files
    csv_blobs = [blob for blob in container_client.list_blobs() if blob.name.endswith('.csv')]

    # Initialize an empty DataFrame
    combined_df = pd.DataFrame()

    # Loop through the CSV blobs, download them, and append their contents to the combined DataFrame
    for blob in csv_blobs:
        # Get a blob client for the current blob
        blob_client = container_client.get_blob_client(blob)
        # Download the blob's contents and load it into a DataFrame
        blob_data = blob_client.download_blob().readall()
        temp_df = pd.read_csv(io.BytesIO(blob_data))
        # Append the temporary DataFrame to the combined DataFrame
        combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

    return combined_df


# Title of the app
st.title('Azure OpenAI Services PayGo Stats')

# A simple text
st.write('In this dashboard, we will test the endpoints of Azure OpenAI services and display the results.')

st.caption("Tthe test sends small request to each endponit found in the resource group, and run against either gpt-4 or gpt-35-turbo model family")

input_model_family = "gpt-4"
input_model_family = st.selectbox("Select model family", ["gpt-4", "gpt-35-turbo"])


# combined_df, aggregated_data = load_data(model_family)

combined_df = load_data_from_blob()

combined_df = combined_df[combined_df['model_family'] ==  input_model_family]

# Assuming 'combined_df' has columns named 'service' and 'duration'
# Aggregate data by 'service' with the average of 'duration'
aggregated_data = combined_df.groupby('service')['duration'].mean().reset_index()


# Step 1: Convert 'start_time' to datetime (if not already)
combined_df['start_time'] = pd.to_datetime(combined_df['start_time'])

# Step 2: Extract date from 'start_time' and create a new column 'run_date'
combined_df['run_date'] = combined_df['start_time'].dt.date

# Step 3: Calculate count of unique dates
unique_dates_count = combined_df['run_date'].nunique()



# Calculate row count of combined_df
row_count = combined_df.shape[0]

st.write(f"So far there has been done {row_count} tests in: {unique_dates_count} days for model family: {input_model_family}")

st.header(f'Average Duration of Requests by Service for Model Family: {input_model_family}')
# Display the aggregated data as a bar chart in the Streamlit app
st.bar_chart(aggregated_data.set_index('service'))

with st.expander("Show raw data"):
    st.write(combined_df)
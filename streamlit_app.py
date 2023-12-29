import streamlit as st
import boto3
from botocore.exceptions import NoCredentialsError

# AWS S3 Configuration
AWS_REGION = 'ap-south-1'
AWS_ACCESS_KEY = 'AKIASTUSGV4NBBOMYGVQ'
AWS_SECRET_KEY = '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'
ORDER_BUCKET_NAME = 'bucket-order'
RETURN_BUCKET_NAME = 'bucket-return'

# Function to upload file to S3
def upload_to_s3(file,bucket_name):
    s3 = boto3.client('s3', region_name=AWS_REGION, aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    try:
        s3.upload_file(file, bucket_name,file)
        st.success(f"File {file} uploaded to {bucket_name} successfully.")
    except FileNotFoundError:
        st.error('This file was not found')
    except NoCredentialsError:
        st.error('Credentials not available')
    except Exception as e:
        st.error(f'An error occurred : {str(e)}')    

# Streamlit UI
def main():
    st.title("Data Upload Streamlit App")

    #File upload
    st.header("Upload Order Data")
    order_file = st.file_uploader("Choose an Order data file", type=["csv", "xlsx"])
    if order_file:
        if st.button("Upload Order Data"):
            upload_to_s3(order_file.name, ORDER_BUCKET_NAME)

    st.header("Upload Return Data")
    return_file = st.file_uploader("Choose a Return data file", type=["csv", "xlsx"])
    if return_file:
        if st.button("Upload Return Data"):
            upload_to_s3(return_file.name, RETURN_BUCKET_NAME)

if __name__ == "__main__":
    main()        
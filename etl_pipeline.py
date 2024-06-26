import pandas as pd
import os
import io
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient


ziko_data = pd.read_csv('ziko_logistics_data.csv')
ziko_data = ziko_data.fillna({
    'Unit_Price':ziko_data['Unit_Price'].mean(),
    'Total_Cost':ziko_data['Total_Cost'].mean(),
    'Discount_Rate': 0.0,
    'Return_Reason':'Unknown'
})

ziko_data['Date'] = pd.to_datetime(ziko_data['Date'])
customer = ziko_data[['Customer_ID','Customer_Name','Customer_Phone', 'Customer_Email', 'Customer_Address']].copy().drop_duplicates().reset_index(drop=True)
product = ziko_data[['Product_ID', 'Quantity','Unit_Price', 'Total_Cost', 'Discount_Rate','Product_List_Title']].copy().drop_duplicates().reset_index(drop=True)
transaction_fact_table = ziko_data.merge(customer, on=['Customer_ID','Customer_Name','Customer_Phone', 'Customer_Email', 'Customer_Address'])\
                                    .merge(product, on= ['Product_ID', 'Quantity','Unit_Price', 'Total_Cost', 'Discount_Rate','Product_List_Title'])\
                                        [['Transaction_ID', 'Date', 'Customer_ID', 'Product_ID','Warehouse_Code', 'Ship_Mode', 'Delivery_Status',\
                                        'Customer_Satisfaction', 'Item_Returned', 'Return_Reason','Sales_Channel',\
                                        'Payment_Type','Taxable', 'Region', 'Country']]
                                        
customer.to_csv(r'datasets/customer.csv', index=False)
product.to_csv(r'datasets/product.csv', index=True)
transaction_fact_table.to_csv(r'datasets/transaction_fact_table.csv', index=False)
print('files have been uploaded as cvs')

load_dotenv()
connect_str = os.getenv('CONNECT_STR')
container_name = os.getenv('CONTAINER_NAME')
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# Get the container client
container_client = blob_service_client.get_container_client(container_name)


def upload_df_blob_parquet(df, container_client, blob_name):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    blob_client= container_client.get_blob_client(blob_name)
    blob_client.upload_blob(buffer, blob_type="BlockBlob", overwrite=True)
    print(f"{blob_name} uploaded to Blob storage successfull")

upload_df_blob_parquet(customer, container_client, 'rawdata/customer.parquet')
upload_df_blob_parquet(product, container_client, 'rawdata/product.parquet')
upload_df_blob_parquet(transaction_fact_table, container_client, 'rawdata/transaction_fact.parquet')


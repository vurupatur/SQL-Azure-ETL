import pandas as pd
import pyodbc, os


#Parameters to connect to
server = '.'
db = 'AAC Data'

#Create the connection
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server
 + ';DATABASE=' + db + ';Trusted_Connection=yes')

#Query the database using a SQL command (select all rows in this case)

sql = """

SELECT * FROM dbo.aac_outcomes 

"""

#Read into pandas df
df = pd.read_sql(sql, conn)

#Print or modify if desired
#print(df.head())
#df.iloc[0:5]

#Specify path and file name, then output to a csv
path = r'C:\Users\delis\Documents\SQL Data'
output_file = os.path.join(path,'AAC_Outcomes_SQL.csv')

df.to_csv(output_file, index = False)


# PART 2 --------------------------------------------------------------------------

#Some code below is borrowed from Microsoft documentation. https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
import uuid, sys
from azure.storage.blob import BlockBlobService, PublicAccess

# Create the BlockBlockService that is used to call the Blob service for the storage account
block_blob_service = BlockBlobService(account_name='delistraty1', account_key='xxx')

# Find my container 'jd-blob1'.
container_name = 'jd-blob1'

# Upload a file
local_file_name = "AAC_Outcomes_SQL.csv" #specify the file to upload
full_path_to_file = os.path.join(path, local_file_name)

print("Temp file = " + full_path_to_file)
print("\nUploading to Blob storage as blob: " + local_file_name)

# Upload the file, use local_file_name for the blob name
block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)

# List the blobs in the container
print("\nList blobs in the container")
generator = block_blob_service.list_blobs(container_name)
for blob in generator:
    print("\t Blob name: " + blob.name)

import ftplib
import datetime
from zipfile import ZipFile
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import snowflake.connector

#Get datetime of last month
today = datetime.date.today()
first = today.replace(day=1)
last_month = first - datetime.timedelta(days=32)
print("GOT DATETIME")

#Name of Zip
zipFilename = f"Inspection_{last_month.year}{last_month.strftime('%b')}.zip"

# #Getting zip file locally
ftp = ftplib.FTP("ftp.senture.com")
ftp.login()
ftp.retrbinary("RETR " + zipFilename, open(zipFilename, 'wb').write)
ftp.quit()
print("DOWNLOADED FILE")

# #Unzipping the file
with ZipFile(f"{zipFilename}", 'r') as zObject: 
# Extracting all the members of the zip 
    zObject.extractall()
    
print("EXTRACTED FILE")

Inspection_txtFilename = f"{last_month.year}{last_month.strftime('%b')}_Inspection.txt"

#sas token through env variable
sas = os.environ.get("SAS_TOKEN")

#Gets a blob_service_client
def get_blob_client_with_sas_url():
    blob_service_client = BlobServiceClient(account_url=f"https://stsnowflakeimports.blob.core.windows.net/", credential=sas)
    return blob_service_client
print("GOT BLOB CLIENT")

#Reads a local file
def read_from_local_system(filename):
    with open(f'{filename}', 'rb') as f:
        binary_content = f.read()
    return binary_content 
print("GOT FILE FROM LOCAL")

#Gets a blob_client and uploads to blob
def upload_to_blob_with_sas_url(byte_data):
    blob_service_client = get_blob_client_with_sas_url()
    blob_client = blob_service_client.get_blob_client("snowflake-imports", f"fmcsa/motor_carrier_inspection_information/{last_month.year}/{str(last_month.month).zfill(2)}/{last_month.year}{last_month.strftime('%b')}_Inspection_test.txt")
    blob_client.upload_blob(byte_data,overwrite=True)
    print("Uploaded the blob")

#Reads the local csv file and uploads to blob
byte_data = read_from_local_system(Inspection_txtFilename)
upload_to_blob_with_sas_url(byte_data)

#Connects to Snowflake
conn = snowflake.connector.connect(
    user='egonzalez@arrivelogistics.com', 
    authenticator="externalbrowser", 
    account='arrive.east-us-2.azure', 
    role="DE_INTERNS", 
    warehouse="DE_INTERNS_MEDIUM",
    database='DAPL_RAW_DEV', 
    schema="DE_INTERNS"
)
print("CONNECTED TO SNOWFLAKE")

#Executes command to copy data over to Snowflake
cur = conn.cursor()
cur.execute(f"""CREATE OR REPLACE TABLE DE_INTERNS.INSPECTION_DATA_{last_month.year}_{last_month.strftime('%b')}(UNIQUE_ID INTEGER  NOT NULL PRIMARY KEY,
            REPORT_NUMBER        VARCHAR(10) NOT NULL,
            REPORT_STATE         VARCHAR(2) NOT NULL
            ,DOT_NUMBER          INTEGER  NOT NULL
            ,INSP_DATE           VARCHAR(9) NOT NULL
            ,INSP_LEVEL_ID       INTEGER  NOT NULL
            ,COUNTY_CODE_STATE   VARCHAR(2) NOT NULL
            ,TIME_WEIGHT         INTEGER  NOT NULL
            ,DRIVER_OOS_TOTAL    INTEGER  NOT NULL
            ,VEHICLE_OOS_TOTAL   INTEGER  NOT NULL
            ,TOTAL_HAZMAT_SENT   INTEGER  NOT NULL
            ,OOS_TOTAL           INTEGER  NOT NULL
            ,HAZMAT_OOS_TOTAL    INTEGER  NOT NULL
            ,HAZMAT_PLACARD_REQ  VARCHAR(30)
            ,UNIT_TYPE_DESC      VARCHAR(18) NOT NULL
            ,UNIT_MAKE           VARCHAR(10)
            ,UNIT_LICENSE        VARCHAR(12)
            ,UNIT_LICENSE_STATE  VARCHAR(2)
            ,VIN                 VARCHAR(17)
            ,UNIT_DECAL_NUMBER   VARCHAR(30)
            ,UNIT_TYPE_DESC2     VARCHAR(18)
            ,UNIT_MAKE2          VARCHAR(10)
            ,UNIT_LICENSE2       VARCHAR(12)
            ,UNIT_LICENSE_STATE2 VARCHAR(2)
            ,VIN2                VARCHAR(17)
            ,UNIT_DECAL_NUMBER2  VARCHAR(30)
            ,UNSAFE_INSP         VARCHAR(1)
            ,FATIGUED_INSP       VARCHAR(1)
            ,DR_FITNESS_INSP     VARCHAR(1)
            ,SUBT_ALCOHOL_INSP   VARCHAR(1)
            ,VH_MAINT_INSP       VARCHAR(1)
            ,HM_INSP             VARCHAR(30)
            ,BASIC_VIOL          INTEGER  NOT NULL
            ,UNSAFE_VIOL         INTEGER  NOT NULL
            ,FATIGUED_VIOL       INTEGER  NOT NULL
            ,DR_FITNESS_VIOL     INTEGER  NOT NULL
            ,SUBT_ALCOHOL_VIOL   INTEGER  NOT NULL
            ,VH_MAINT_VIOL       INTEGER  NOT NULL
            ,HM_VIOL             NUMERIC(3,1));""")
print("CREATED DATA TABLE")

cur.execute(f"copy into DE_INTERNS.INSPECTION_DATA_{last_month.year}_{last_month.strftime('%b')} FROM 'azure://stsnowflakeimports.blob.core.windows.net/snowflake-imports/fmcsa/motor_carrier_inspection_information/{last_month.year}/{str(last_month.month).zfill(2)}/{last_month.year}{last_month.strftime('%b')}_Inspection_test.txt'   credentials=(azure_sas_token='{sas}')  FORCE = TRUE file_format = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' NULL_IF = ('\\N', '') SKIP_HEADER = 1 ESCAPE_UNENCLOSED_FIELD = None ENCODING = WINDOWS1252) ;")
print("UPLOADED INTO TABLE")

#Commits changes?
print (cur.fetchone())
cur.execute(f" Commit  ;")
cur.close()
conn.close()
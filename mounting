adlsAccountName="stglandingzone"
adlsContainerName="rawdata"
adlsFolderName="Movie"
mountPoint="/mnt/Files/Rawdata"
#Application client Id
applicationId=dbutils.secrets.get(scope="moviesec",key="clientid")
# Application client secret key
authenticationKey=dbutils.secrets.get(scope="moviesec",key="clientsecret")
# directory tenant id
tenandId=dbutils.secrets.get(scope="moviesec",key="tenantid")
endpoint = "https://login.microsoftonline.com/" + tenandId + "/oauth2/token"
source = "abfss://" + adlsContainerName + "@" + adlsAccountName + ".dfs.core.windows.net/" + adlsFolderName

# Connecting using Service Principal secrets and OAuth
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationId,
           "fs.azure.account.oauth2.client.secret": authenticationKey,
           "fs.azure.account.oauth2.client.endpoint": endpoint}

# Mounting ADLS Storage to DBFS
# Mount only if the directory is not already mounted
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  dbutils.fs.mount(
    source = source,
    mount_point = mountPoint,
    extra_configs = configs)

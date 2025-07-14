import os
import boto3
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime, timedelta
from dotenv import load_dotenv

# --- Load AWS and Snowflake credentials from env ---

load_dotenv()
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")



sf_config = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"]
}

# --- Step 1: Connect to Snowflake ---
print("Connecting to Snowflake...")
session = Session.builder.configs(sf_config).create()

# --- Step 2: Load PAT table ---
df = session.table("PAT_EXPIRY_LOG").to_pandas()
df.columns = df.columns.str.upper()

# --- Step 3: Filter tokens expiring in 7 days ---
now = datetime.utcnow()
cutoff = now + timedelta(days=7)

df["EXPIRES_AT"] = pd.to_datetime(df["EXPIRES_AT"], errors="coerce")
df["NOTIFIED_ON"] = pd.to_datetime(df["NOTIFIED_ON"], errors="coerce")

expiring_soon = df[df["EXPIRES_AT"].notna() & (df["EXPIRES_AT"] <= cutoff) & (df["EXPIRES_AT"] >= now)].copy()

if expiring_soon.empty:
    print("No PATs expiring in the next 7 days.")
    exit(0)

# --- Step 4: Format message for email ---
message_lines = ["PAT Tokens Expiring Within 7 Days:\n"]
for _, row in expiring_soon.iterrows():
    message_lines.append(f"Token: {row['TOKEN_NAME']} | User: {row['USER_NAME']} | Expires At: {row['EXPIRES_AT']:%Y-%m-%d %H:%M UTC}")

message = "\n".join(message_lines)

# --- Step 5: Send SNS Notification ---
print("Sending email notification via AWS SNS...")

sns_client = boto3.client("sns")
sns_client.publish(
    TopicArn=SNS_TOPIC_ARN,
    Subject="PAT Tokens Expiring Soon in Snowflake",
    Message=message
)

print("Email sent!")

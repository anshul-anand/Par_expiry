# notify_pat_data.py

import os
import boto3
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime, timedelta

# --- Step 1: Load environment variables ---
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

sf_config = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"]
}

# --- Step 2: Connect to Snowflake ---
print("🔗 Connecting to Snowflake...")
session = Session.builder.configs(sf_config).create()

# --- Step 3: Load PAT_EXPIRY_LOG data ---
df = session.table("PAT_EXPIRY_LOG").to_pandas()
df.columns = df.columns.str.upper()

# --- Step 4: Filter expiring tokens within next 7 days ---
now = datetime.utcnow()
cutoff = now + timedelta(days=7)

df["EXPIRES_AT"] = pd.to_datetime(df["EXPIRES_AT"], errors="coerce")
df["NOTIFIED_ON"] = pd.to_datetime(df["NOTIFIED_ON"], errors="coerce")

expiring_soon = df[df["EXPIRES_AT"].notna() & (df["EXPIRES_AT"] <= cutoff) & (df["EXPIRES_AT"] >= now)].copy()

if expiring_soon.empty:
    print("No PATs expiring in the next 7 days.")
    exit(0)

# --- Step 5: Format email body ---
message_lines = ["*PAT Tokens Expiring Within 7 Days:*\n"]
for _, row in expiring_soon.iterrows():
    message_lines.append(
        f"- User: {row['USER_NAME']} | Token: {row['TOKEN_NAME']} | Expires: {row['EXPIRES_AT']:%Y-%m-%d %H:%M UTC}"
    )

email_body = "\n".join(message_lines)

# --- Step 6: Send Email via AWS SNS ---
print("Sending email via AWS SNS...")

sns = boto3.client(
    'sns',
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ["AWS_DEFAULT_REGION"]
)

sns.publish(
    TopicArn=SNS_TOPIC_ARN,
    Subject="PAT Tokens Expiring Soon in Snowflake",
    Message=email_body
)

print("Email sent!")

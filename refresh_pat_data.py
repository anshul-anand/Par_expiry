import os
import snowflake.connector
from datetime import datetime

def connect_to_snowflake():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")  # Optional
    )

def fetch_users_with_pats(cursor):
    cursor.execute("SHOW USERS")
    users = cursor.fetchall()
    cols = [col[0] for col in cursor.description]
    user_dicts = [dict(zip(cols, row)) for row in users]
    return [user["name"] for user in user_dicts if user.get("has_pat") == 'true']

def fetch_user_tokens(cursor, user):
    try:
        cursor.execute(f'SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER "{user}"')
        tokens = cursor.fetchall()
        cols = [col[0] for col in cursor.description]
        return [dict(zip(cols, row)) for row in tokens]
    except Exception:
        return []  # Skip if can't fetch

def insert_pat_if_not_exists(cursor, token, user):
    token_name = token["name"]
    created_on = token["created_on"].replace(tzinfo=None)
    expires_at = token["expires_at"].replace(tzinfo=None)
    notified_on = datetime.utcnow()

    check_sql = """
        SELECT 1 FROM PAT_EXPIRY_LOG
        WHERE token_name = %s
          AND user_name = %s
          AND created_on = %s
          AND expires_at = %s
    """
    cursor.execute(check_sql, (token_name, user, created_on, expires_at))
    exists = cursor.fetchone()
    if exists:
        return False

    insert_sql = """
        INSERT INTO PAT_EXPIRY_LOG (token_name, user_name, created_on, expires_at, notified_on)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(insert_sql, (token_name, user, created_on, expires_at, notified_on))
    return True

def main():
    print("⏳ Connecting to Snowflake...")
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    inserted_count = 0

    try:
        users = fetch_users_with_pats(cursor)
        print(f"👤 Found {len(users)} users with PATs")

        for user in users:
            tokens = fetch_user_tokens(cursor, user)
            for token in tokens:
                if insert_pat_if_not_exists(cursor, token, user):
                    inserted_count += 1

        print(f"✅ Done! Inserted {inserted_count} new PAT tokens.")
        conn.commit()

    except Exception as e:
        print(f"❌ Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()

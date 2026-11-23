import os
import asyncio
import logging

import discord
from discord.ext import commands
import psycopg2
import functions_framework
from google.cloud import secretmanager

# Local testing only: load a .env file if python-dotenv is installed.
# In Cloud Functions this is skipped (no .env file, package not installed).
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Logging (unchanged from your original script)
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(message)s')
logging.getLogger('discord').propagate = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# Non-sensitive values are passed in as plain environment variables at deploy
# time (--set-env-vars). Sensitive values (bot token, db password) are pulled
# from Secret Manager at request time instead of a local .env file.
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER")
GUILD_NAME = os.environ.get("GUILD_NAME")

# Maps each Secret Manager secret name to the env var used when testing locally
# (these match the variable names already in your original .env file).
LOCAL_ENV_NAMES = {
    "discord-token": "DISCORD_BOT_TOKEN",
    "db-password": "DB_PASSWORD",
}

_secret_client = None


def get_secret(secret_id: str) -> str:
    """
    Return a secret value.
    1. Local testing: if the matching env var / .env entry exists, use it.
    2. Cloud: otherwise fetch the latest version from Secret Manager.
    """
    local_value = os.environ.get(LOCAL_ENV_NAMES.get(secret_id, ""))
    if local_value:
        return local_value

    global _secret_client
    if _secret_client is None:
        _secret_client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = _secret_client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def connect_to_db():
    """Connect to the Supabase PostgreSQL database (transaction pooler)."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=get_secret("db-password"),
            port=6543,            # Supabase transaction pooler port
            sslmode="require",    # Supabase requires SSL
        )
        logging.info("Successfully connected to Database...")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        return None


def reassign_event_ids(conn):
    """
    Reassigns event_id values based on the chronological order of event_datetime.
    (Logic unchanged from your original script.)
    """
    try:
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE member_events DROP CONSTRAINT member_events_pkey;")
        conn.commit()
        cursor.execute("""
            WITH ordered_events AS (
                SELECT event_id, ROW_NUMBER() OVER (ORDER BY event_datetime) AS new_event_id
                FROM member_events
            )
            UPDATE member_events
            SET event_id = ordered_events.new_event_id
            FROM ordered_events
            WHERE member_events.event_id = ordered_events.event_id;
        """)
        conn.commit()
        cursor.execute("ALTER TABLE member_events ADD PRIMARY KEY (event_id);")
        conn.commit()
        cursor.execute(
            "SELECT setval('member_events_event_id_seq', (SELECT MAX(event_id) FROM member_events), true);"
        )
        conn.commit()
        logging.info("Successfully updated member_events table...")
    except psycopg2.Error as e:
        logging.error(f"Error reassigning event_id values: {e}")
        conn.rollback()


async def run_discord_sync() -> dict:
    """
    Connects to Discord as a bot, pulls the current member list, diffs it
    against the database, and writes the changes to Supabase.
    Returns a status dict so the Cloud Function entry point can build a
    meaningful HTTP response instead of just logging to stdout.
    """
    intents = discord.Intents.default()
    intents.members = True
    intents.presences = True
    intents.message_content = True
    bot = commands.Bot(command_prefix="!", intents=intents)

    outcome = {"status": "error", "detail": "on_ready never fired"}

    @bot.event
    async def on_ready():
        logging.info(f"\n{'-'*50}\nBot connected as {bot.user}...")
        guild = discord.utils.get(bot.guilds, name=GUILD_NAME)
        if not guild:
            logging.error(f"Guild '{GUILD_NAME}' not found.")
            outcome.update(status="error", detail=f"Guild '{GUILD_NAME}' not found")
            await bot.close()
            return

        conn = connect_to_db()
        if not conn:
            outcome.update(status="error", detail="Database connection failed")
            await bot.close()
            return

        try:
            cursor = conn.cursor()

            # Fetch current Discord members
            current_members = {member.id: member for member in guild.members}

            # Fetch known members from database
            cursor.execute("SELECT discord_id FROM server_members")
            known_members = {row[0] for row in cursor.fetchall()}

            current_ids = set(current_members.keys())
            new_ids = current_ids - known_members
            left_ids = known_members - current_ids
            existing_ids = current_ids & known_members

            new_members_count = 0
            left_members_count = 0
            rejoined_members_count = 0

            # Process new members (including rejoins)
            for member_id in new_ids:
                member = current_members[member_id]
                cursor.execute("SELECT 1 FROM former_members WHERE discord_id = %s", (member_id,))
                if cursor.fetchone():
                    rejoined_members_count += 1
                    cursor.execute("DELETE FROM former_members WHERE discord_id = %s", (member_id,))
                else:
                    new_members_count += 1

                member_since = member.joined_at.isoformat() if member.joined_at else None
                boosting_since = member.premium_since.isoformat() if member.premium_since else None
                joined_discord = member.created_at.isoformat()

                cursor.execute("""
                    INSERT INTO server_members (
                        discord_id, username, display_name, top_role, current_status,
                        member_since, roles, num_roles, boosting_since, joined_discord,
                        permissions, screening_pending
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (discord_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        display_name = EXCLUDED.display_name,
                        top_role = EXCLUDED.top_role,
                        current_status = EXCLUDED.current_status,
                        member_since = EXCLUDED.member_since,
                        roles = EXCLUDED.roles,
                        num_roles = EXCLUDED.num_roles,
                        boosting_since = EXCLUDED.boosting_since,
                        permissions = EXCLUDED.permissions,
                        screening_pending = EXCLUDED.screening_pending;
                """, (
                    member.id, member.name, member.display_name,
                    member.top_role.name if member.top_role else None,
                    str(member.status), member_since,
                    ','.join(role.name for role in member.roles), len(member.roles),
                    boosting_since, joined_discord,
                    str(member.guild_permissions), member.pending
                ))

                cursor.execute("""
                    INSERT INTO member_events (discord_id, event_type, event_datetime)
                    VALUES (%s, 1, %s)
                """, (member_id, member_since))

            # Process existing members (update details)
            for member_id in existing_ids:
                member = current_members[member_id]
                boosting_since = member.premium_since.isoformat() if member.premium_since else None

                cursor.execute("""
                    UPDATE server_members SET
                        username = %s,
                        display_name = %s,
                        top_role = %s,
                        current_status = %s,
                        roles = %s,
                        num_roles = %s,
                        boosting_since = %s,
                        permissions = %s,
                        screening_pending = %s
                    WHERE discord_id = %s
                """, (
                    member.name, member.display_name,
                    member.top_role.name if member.top_role else None,
                    str(member.status),
                    ','.join(role.name for role in member.roles), len(member.roles),
                    boosting_since,
                    str(member.guild_permissions), member.pending,
                    member_id
                ))

            # Process members who left
            for member_id in left_ids:
                left_members_count += 1
                cursor.execute("""
                    INSERT INTO former_members (
                        discord_id, username, display_name, top_role, joined_at,
                        roles, num_roles, boosting_since, joined_discord,
                        permissions, screening_pending, left_before
                    )
                    SELECT
                        discord_id, username, display_name, top_role, member_since,
                        roles, num_roles, boosting_since, joined_discord,
                        permissions, screening_pending, NOW() AT TIME ZONE 'UTC'
                    FROM server_members
                    WHERE discord_id = %s
                    ON CONFLICT (discord_id) DO UPDATE SET
                        left_before = EXCLUDED.left_before;
                """, (member_id,))
                cursor.execute("DELETE FROM server_members WHERE discord_id = %s", (member_id,))
                cursor.execute("""
                    INSERT INTO member_events (discord_id, event_type, event_datetime)
                    VALUES (%s, -1, NOW() AT TIME ZONE 'UTC')
                """, (member_id,))

            conn.commit()
            reassign_event_ids(conn)

            cursor.execute("SELECT COUNT(*) FROM server_members")
            server_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM former_members")
            former_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM member_events")
            events_count = cursor.fetchone()[0]

            logging.info(
                f"\nMembers Updates:\n New = {new_members_count}\n "
                f"Left = {left_members_count}\n Rejoined = {rejoined_members_count}"
            )
            logging.info(
                f"\nFinal Count:\n Server Members = {server_count}\n "
                f"Former Members = {former_count}\n Member Events = {events_count}"
            )
            logging.info("\nData Updated Successfully!!")

            outcome.update(
                status="success",
                detail=(
                    f"new={new_members_count}, left={left_members_count}, "
                    f"rejoined={rejoined_members_count}, total_members={server_count}"
                ),
            )
        except Exception as e:
            logging.error(f"Error: {e}")
            conn.rollback()
            outcome.update(status="error", detail=str(e))
        finally:
            cursor.close()
            conn.close()
            await bot.close()

    bot_token = get_secret("discord-token")
    await bot.start(bot_token)
    return outcome


@functions_framework.http
def run_discord_pull(request):
    """HTTP entry point. Cloud Scheduler calls this on a timer."""
    try:
        result = asyncio.run(run_discord_sync())
    except Exception as e:
        logging.error(f"Fatal error running Discord sync: {e}")
        return f"Failed: {e}", 500

    if result["status"] == "success":
        return f"Discord data pull completed: {result['detail']}", 200
    return f"Discord data pull failed: {result['detail']}", 500


# ---------------------------------------------------------------------------
# Local testing entry point: lets you run `python Trial1.py` on your laptop.
# Cloud Functions ignores this block (it imports the file and calls
# run_discord_pull directly, so __name__ is not "__main__" there).
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    result = asyncio.run(run_discord_sync())
    logging.info(f"\nRESULT: {result['status'].upper()} - {result['detail']}")
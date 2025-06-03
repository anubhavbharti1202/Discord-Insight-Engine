import discord
from discord.ext import commands
import os
import psycopg2
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
GUILD_NAME = os.getenv("GUILD_NAME")

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logging.getLogger('discord').propagate = False

# Define intents
intents = discord.Intents.default()
intents.members = True
intents.presences = True
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)


def connect_to_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=5433
        )
        logging.info("Successfully connected to Database...")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        return None


def reassign_event_ids(conn):
    """
    Reassigns event_id values based on the chronological order of event_datetime.
    """
    try:
        cursor = conn.cursor()

        # Temporarily remove the primary key constraint
        cursor.execute("ALTER TABLE member_events DROP CONSTRAINT member_events_pkey;")
        conn.commit()

        # Reassign event_id values in chronological order
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

        # Re-add the primary key constraint
        cursor.execute("ALTER TABLE member_events ADD PRIMARY KEY (event_id);")
        conn.commit()

        # Reset the sequence for event_id
        cursor.execute("SELECT setval('member_events_event_id_seq', (SELECT MAX(event_id) FROM member_events), true);")
        conn.commit()

        logging.info("Successfully updated member_events table...")
    except psycopg2.Error as e:
        logging.error(f"Error reassigning event_id values: {e}")
        conn.rollback()


@bot.event
async def on_ready():
    """Main logic when the bot is ready."""
    logging.info(f"\n{'-'*50}\nBot connected as {bot.user}...")
    guild = discord.utils.get(bot.guilds, name=GUILD_NAME)
    if not guild:
        logging.error(f"Guild '{GUILD_NAME}' not found.")
        await bot.close()
        return

    conn = connect_to_db()
    if not conn:
        await bot.close()
        return

    try:
        cursor = conn.cursor()

        # Fetch current Discord members
        current_members = {member.id: member for member in guild.members}

        # Fetch known members from database
        cursor.execute("SELECT discord_id FROM server_members")
        known_members = {row[0] for row in cursor.fetchall()}

        # Identify new, existing, and left members
        current_ids = set(current_members.keys())
        new_ids = current_ids - known_members  # Members in Discord but not in DB
        left_ids = known_members - current_ids  # Members in DB but not in Discord
        existing_ids = current_ids & known_members  # Members in both Discord and DB

        # Initialize counters
        new_members_count = 0
        left_members_count = 0
        rejoined_members_count = 0

        # Process new members (including rejoins)
        for member_id in new_ids:
            member = current_members[member_id]
            # Check if rejoining from former_members
            cursor.execute("SELECT 1 FROM former_members WHERE discord_id = %s", (member_id,))
            if cursor.fetchone():
                rejoined_members_count += 1
                cursor.execute("DELETE FROM former_members WHERE discord_id = %s", (member_id,))
            else:
                new_members_count += 1

            # Convert datetime objects to ISO format strings
            member_since = member.joined_at.isoformat() if member.joined_at else None
            boosting_since = member.premium_since.isoformat() if member.premium_since else None
            joined_discord = member.created_at.isoformat()

            # Insert or update server_members
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

            # Log join event
            cursor.execute("""
                INSERT INTO member_events (discord_id, event_type, event_datetime)
                VALUES (%s, 1, %s)
            """, (member_id, member_since))

        # Process existing members (update details)
        for member_id in existing_ids:
            member = current_members[member_id]
            # Convert datetime objects to ISO format strings
            member_since = member.joined_at.isoformat() if member.joined_at else None
            boosting_since = member.premium_since.isoformat() if member.premium_since else None
            joined_discord = member.created_at.isoformat()

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

            # Move to former_members
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
            # Remove from server_members
            cursor.execute("DELETE FROM server_members WHERE discord_id = %s", (member_id,))
            # Log leave event
            cursor.execute("""
                INSERT INTO member_events (discord_id, event_type, event_datetime)
                VALUES (%s, -1, NOW() AT TIME ZONE 'UTC')
            """, (member_id,))

        conn.commit()

        # Reassign event_id values in chronological order
        reassign_event_ids(conn)

        # Log final counts
        cursor.execute("SELECT COUNT(*) FROM server_members")
        server_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM former_members")
        former_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM member_events")
        events_count = cursor.fetchone()[0]

        logging.info(f"\nMembers Updates:\n New = {new_members_count}\n Left = {left_members_count}\n Rejoined = {rejoined_members_count}")
        logging.info(f"\nFinal Count:\n Server Members = {server_count}\n Former Members = {former_count}\n Member Events = {events_count}")
        logging.info("\nData Updated Successfully!!")

    except Exception as e:
        logging.error(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        await bot.close()


if __name__ == "__main__":
    if not BOT_TOKEN:
        logging.error("Bot token missing. Check .env file.")
    else:
        bot.run(BOT_TOKEN)

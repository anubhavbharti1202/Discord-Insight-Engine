import os
import asyncio
import logging

import discord
from discord.ext import commands
import psycopg2
import functions_framework
from google.cloud import secretmanager

# Local testing only: load a .env file if python-dotenv is installed.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger("discord").propagate = False


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME", "postgres")
DB_USER = os.environ.get("DB_USER")
GUILD_NAME = os.environ.get("GUILD_NAME")

LOCAL_ENV_NAMES = {
    "discord-token": "DISCORD_BOT_TOKEN",
    "db-password": "DB_PASSWORD",
}

_secret_client = None


def get_secret(secret_id: str) -> str:
    """
    Return a secret value.
    1. Local testing: use the matching environment variable / .env entry.
    2. Cloud: fetch the latest version from Secret Manager.
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
    """Connect to Supabase PostgreSQL through its transaction pooler."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=get_secret("db-password"),
            port=6543,
            sslmode="require",
            connect_timeout=10,
        )
        logging.info("Successfully connected to Database...")
        return conn
    except psycopg2.Error as error:
        logging.error("Database connection error: %s", error)
        return None


async def run_discord_sync() -> dict:
    """
    Pull a complete Discord member snapshot, compare it with PostgreSQL,
    and persist new joins, departures, rejoins, and changed member details.

    Existing event IDs are never rewritten. PostgreSQL assigns each new
    member_events.event_id through the table's existing sequence.
    """
    intents = discord.Intents.default()
    intents.members = True
    intents.presences = True
    # message_content is not required for this scheduled member-sync job.

    bot = commands.Bot(command_prefix="!", intents=intents)
    outcome = {"status": "error", "detail": "on_ready never fired"}
    sync_started = False

    @bot.event
    async def on_ready():
        nonlocal sync_started

        # on_ready can fire again after a reconnect. Run this scheduled sync
        # only once during this invocation.
        if sync_started:
            return
        sync_started = True

        conn = None
        cursor = None

        try:
            logging.info("\n%s\nBot connected as %s...", "-" * 50, bot.user)

            guild = discord.utils.get(bot.guilds, name=GUILD_NAME)
            if guild is None:
                raise RuntimeError(f"Guild '{GUILD_NAME}' not found.")

            # Use the existing cache when discord.py confirms it contains
            # the full guild membership. Otherwise request all members.
            # Intents.members=True is required for accurate member chunking.
            if guild.chunked:
                logging.info(
                    "Guild member cache is complete. Using cached members."
                )
                current_member_list = guild.members
            else:
                logging.info(
                    "Guild member cache is incomplete. "
                    "Requesting the full member list from Discord..."
                )
                current_member_list = await guild.chunk(cache=True)

            current_members = {
                member.id: member for member in current_member_list
            }

            # Do not interpret an incomplete snapshot as member departures.
            # Discord's count and member-list response are not an atomic
            # snapshot, so a member joining/leaving during retrieval can
            # briefly cause a mismatch. In that case, abort safely and retry
            # on the next scheduled run rather than modifying database state.
            expected_member_count = guild.member_count
            if (
                expected_member_count is not None
                and len(current_members) != expected_member_count
            ):
                raise RuntimeError(
                    "Discord member snapshot count mismatch: "
                    f"retrieved {len(current_members)} members, "
                    f"Discord currently reports {expected_member_count}. "
                    "No database changes were made; retry on the next sync."
                )

            conn = connect_to_db()
            if conn is None:
                raise RuntimeError("Database connection failed.")

            cursor = conn.cursor()

            # Fetch all known active member IDs once.
            cursor.execute("SELECT discord_id FROM server_members")
            known_members = {row[0] for row in cursor.fetchall()}

            current_ids = set(current_members)
            new_ids = current_ids - known_members
            left_ids = known_members - current_ids
            existing_ids = current_ids & known_members

            new_members_count = 0
            left_members_count = 0
            rejoined_members_count = 0
            changed_existing_members_count = 0

            # Fetch former-member IDs for this batch of new IDs in one query,
            # rather than issuing one SELECT for every new member.
            former_member_ids = set()
            if new_ids:
                cursor.execute(
                    """
                    SELECT discord_id
                    FROM former_members
                    WHERE discord_id = ANY(%s)
                    """,
                    (list(new_ids),),
                )
                former_member_ids = {row[0] for row in cursor.fetchall()}

            # ----------------------------------------------------------------
            # Process new members, including returning former members.
            # ----------------------------------------------------------------
            for member_id in new_ids:
                member = current_members[member_id]

                if member_id in former_member_ids:
                    rejoined_members_count += 1
                    cursor.execute(
                        "DELETE FROM former_members WHERE discord_id = %s",
                        (member_id,),
                    )
                else:
                    new_members_count += 1

                member_since = (
                    member.joined_at.isoformat() if member.joined_at else None
                )
                boosting_since = (
                    member.premium_since.isoformat()
                    if member.premium_since else None
                )
                joined_discord = member.created_at.isoformat()

                cursor.execute(
                    """
                    INSERT INTO server_members (
                        discord_id, username, display_name, top_role,
                        current_status, member_since, roles, num_roles,
                        boosting_since, joined_discord, permissions,
                        screening_pending
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        screening_pending = EXCLUDED.screening_pending
                    WHERE
                        server_members.username
                            IS DISTINCT FROM EXCLUDED.username
                        OR server_members.display_name
                            IS DISTINCT FROM EXCLUDED.display_name
                        OR server_members.top_role
                            IS DISTINCT FROM EXCLUDED.top_role
                        OR server_members.current_status
                            IS DISTINCT FROM EXCLUDED.current_status
                        OR server_members.member_since
                            IS DISTINCT FROM EXCLUDED.member_since
                        OR server_members.roles
                            IS DISTINCT FROM EXCLUDED.roles
                        OR server_members.num_roles
                            IS DISTINCT FROM EXCLUDED.num_roles
                        OR server_members.boosting_since
                            IS DISTINCT FROM EXCLUDED.boosting_since
                        OR server_members.permissions
                            IS DISTINCT FROM EXCLUDED.permissions
                        OR server_members.screening_pending
                            IS DISTINCT FROM EXCLUDED.screening_pending
                    """,
                    (
                        member.id,
                        member.name,
                        member.display_name,
                        member.top_role.name if member.top_role else None,
                        str(member.status),
                        member_since,
                        ",".join(role.name for role in member.roles),
                        len(member.roles),
                        boosting_since,
                        joined_discord,
                        str(member.guild_permissions),
                        member.pending,
                    ),
                )

                # Preserve the original join-event timestamp: Discord's
                # actual member.joined_at value.
                cursor.execute(
                    """
                    INSERT INTO member_events (
                        discord_id, event_type, event_datetime
                    )
                    VALUES (%s, 1, %s)
                    """,
                    (member_id, member_since),
                )

            # ----------------------------------------------------------------
            # Update existing members only when tracked values have changed.
            # member_since and joined_discord remain untouched, as before.
            # ----------------------------------------------------------------
            for member_id in existing_ids:
                member = current_members[member_id]
                member_values = (
                    member.name,
                    member.display_name,
                    member.top_role.name if member.top_role else None,
                    str(member.status),
                    ",".join(role.name for role in member.roles),
                    len(member.roles),
                    (
                        member.premium_since.isoformat()
                        if member.premium_since else None
                    ),
                    str(member.guild_permissions),
                    member.pending,
                )

                cursor.execute(
                    """
                    UPDATE server_members
                    SET
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
                      AND (
                        username IS DISTINCT FROM %s
                        OR display_name IS DISTINCT FROM %s
                        OR top_role IS DISTINCT FROM %s
                        OR current_status IS DISTINCT FROM %s
                        OR roles IS DISTINCT FROM %s
                        OR num_roles IS DISTINCT FROM %s
                        OR boosting_since IS DISTINCT FROM %s
                        OR permissions IS DISTINCT FROM %s
                        OR screening_pending IS DISTINCT FROM %s
                      )
                    """,
                    member_values
                    + (member_id,)
                    + member_values,
                )

                if cursor.rowcount == 1:
                    changed_existing_members_count += 1

            # ----------------------------------------------------------------
            # Process members who left.
            # Preserve the original leave timestamp semantics:
            # detection time using NOW() AT TIME ZONE 'UTC'.
            # ----------------------------------------------------------------
            for member_id in left_ids:
                cursor.execute(
                    """
                    INSERT INTO former_members (
                        discord_id, username, display_name, top_role,
                        joined_at, roles, num_roles, boosting_since,
                        joined_discord, permissions, screening_pending,
                        left_before
                    )
                    SELECT
                        discord_id, username, display_name, top_role,
                        member_since, roles, num_roles, boosting_since,
                        joined_discord, permissions, screening_pending,
                        NOW() AT TIME ZONE 'UTC'
                    FROM server_members
                    WHERE discord_id = %s
                    ON CONFLICT (discord_id) DO UPDATE SET
                        left_before = EXCLUDED.left_before
                    """,
                    (member_id,),
                )

                cursor.execute(
                    "DELETE FROM server_members WHERE discord_id = %s",
                    (member_id,),
                )

                cursor.execute(
                    """
                    INSERT INTO member_events (
                        discord_id, event_type, event_datetime
                    )
                    VALUES (%s, -1, NOW() AT TIME ZONE 'UTC')
                    """,
                    (member_id,),
                )
                left_members_count += 1

            # Read final counts inside the same transaction, then commit once.
            cursor.execute("SELECT COUNT(*) FROM server_members")
            server_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM former_members")
            former_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM member_events")
            events_count = cursor.fetchone()[0]

            conn.commit()

            logging.info(
                "\nMembers Updates:\n"
                " New = %s\n Left = %s\n Rejoined = %s\n"
                " Existing members changed = %s",
                new_members_count,
                left_members_count,
                rejoined_members_count,
                changed_existing_members_count,
            )
            logging.info(
                "\nFinal Count:\n"
                " Server Members = %s\n Former Members = %s\n"
                " Member Events = %s",
                server_count,
                former_count,
                events_count,
            )
            logging.info("\nData Updated Successfully!!")

            outcome.update(
                status="success",
                detail=(
                    f"new={new_members_count}, "
                    f"left={left_members_count}, "
                    f"rejoined={rejoined_members_count}, "
                    f"total_members={server_count}"
                ),
            )

        except Exception as error:
            logging.exception("Error during Discord member sync")
            if conn is not None:
                conn.rollback()
            outcome.update(status="error", detail=str(error))

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
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
    except Exception as error:
        logging.exception("Fatal error running Discord sync")
        return f"Failed: {error}", 500

    if result["status"] == "success":
        return f"Discord data pull completed: {result['detail']}", 200
    return f"Discord data pull failed: {result['detail']}", 500


# ---------------------------------------------------------------------------
# Local testing entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    result = asyncio.run(run_discord_sync())
    logging.info(
        "\nRESULT: %s - %s",
        result["status"].upper(),
        result["detail"],
    )

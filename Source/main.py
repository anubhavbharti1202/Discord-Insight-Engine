import os
import asyncio
import logging
import time

import discord
from discord.ext import commands
import psycopg2
from psycopg2.extras import execute_values
import functions_framework

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
    1. Local testing / env-var secrets: use the matching environment variable
       (or .env entry). On Cloud Run functions this is what
       `--set-secrets DISCORD_BOT_TOKEN=discord-token:latest,...` provides.
    2. Otherwise: fetch the latest version from Secret Manager.
    """
    local_value = os.environ.get(LOCAL_ENV_NAMES.get(secret_id, ""))
    if local_value:
        return local_value

    global _secret_client
    if _secret_client is None:
        # Imported lazily so the (heavy) Secret Manager client is only loaded
        # when a secret is not supplied through an environment variable.
        from google.cloud import secretmanager

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


# Maximum number of rows (or IDs) sent to PostgreSQL in a single statement.
BATCH_SIZE = 500


def chunked(items, size=BATCH_SIZE):
    """Yield fixed-size slices of a list so no single statement grows unbounded."""
    for start in range(0, len(items), size):
        yield items[start:start + size]


async def run_discord_sync() -> dict:
    """
    Pull a complete Discord member snapshot, compare it with PostgreSQL,
    and persist new joins, departures, rejoins, and changed member details.

    Existing event IDs are never rewritten. PostgreSQL assigns each new
    member_events.event_id through the table's existing sequence.

    All database writes are sent as set-based / multi-row statements rather
    than one statement per member, and everything is committed once.
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
        started_at = time.perf_counter()

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

            # A new ID that is already in former_members is a rejoin.
            rejoined_ids = new_ids & former_member_ids
            rejoined_members_count = len(rejoined_ids)
            new_members_count = len(new_ids) - rejoined_members_count

            # ----------------------------------------------------------------
            # Process new members, including returning former members.
            # ----------------------------------------------------------------

            # Returning members are removed from former_members first
            # (same DELETE as before, one statement per batch of IDs).
            for batch in chunked(sorted(rejoined_ids)):
                cursor.execute(
                    "DELETE FROM former_members WHERE discord_id = ANY(%s)",
                    (batch,),
                )

            new_member_rows = []
            join_event_rows = []
            for member_id in sorted(new_ids):
                member = current_members[member_id]

                member_since = (
                    member.joined_at.isoformat() if member.joined_at else None
                )
                boosting_since = (
                    member.premium_since.isoformat()
                    if member.premium_since else None
                )
                joined_discord = member.created_at.isoformat()

                new_member_rows.append(
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
                    )
                )

                # Preserve the original join-event timestamp: Discord's
                # actual member.joined_at value.
                join_event_rows.append((member_id, member_since))

            if new_member_rows:
                execute_values(
                    cursor,
                    """
                    INSERT INTO server_members (
                        discord_id, username, display_name, top_role,
                        current_status, member_since, roles, num_roles,
                        boosting_since, joined_discord, permissions,
                        screening_pending
                    )
                    VALUES %s
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
                    new_member_rows,
                    page_size=BATCH_SIZE,
                )

                execute_values(
                    cursor,
                    """
                    INSERT INTO member_events (
                        discord_id, event_type, event_datetime
                    )
                    VALUES %s
                    """,
                    join_event_rows,
                    template="(%s, 1, %s)",
                    page_size=BATCH_SIZE,
                )

            # ----------------------------------------------------------------
            # Update existing members only when tracked values have changed.
            # member_since and joined_discord remain untouched, as before.
            #
            # The current values are loaded into a transaction-scoped temp
            # table (typed from server_members itself, dropped automatically
            # at commit/rollback), then applied with one UPDATE ... FROM that
            # only touches rows whose tracked values differ.
            # ----------------------------------------------------------------
            if existing_ids:
                cursor.execute(
                    """
                    CREATE TEMP TABLE tmp_existing_members
                    ON COMMIT DROP AS
                    SELECT
                        discord_id, username, display_name, top_role,
                        current_status, roles, num_roles, boosting_since,
                        permissions, screening_pending
                    FROM server_members
                    WITH NO DATA
                    """
                )

                existing_member_rows = []
                for member_id in sorted(existing_ids):
                    member = current_members[member_id]
                    existing_member_rows.append(
                        (
                            member_id,
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
                    )

                execute_values(
                    cursor,
                    """
                    INSERT INTO tmp_existing_members (
                        discord_id, username, display_name, top_role,
                        current_status, roles, num_roles, boosting_since,
                        permissions, screening_pending
                    )
                    VALUES %s
                    """,
                    existing_member_rows,
                    page_size=BATCH_SIZE,
                )

                cursor.execute(
                    """
                    UPDATE server_members AS s
                    SET
                        username = t.username,
                        display_name = t.display_name,
                        top_role = t.top_role,
                        current_status = t.current_status,
                        roles = t.roles,
                        num_roles = t.num_roles,
                        boosting_since = t.boosting_since,
                        permissions = t.permissions,
                        screening_pending = t.screening_pending
                    FROM tmp_existing_members AS t
                    WHERE s.discord_id = t.discord_id
                      AND (
                        s.username IS DISTINCT FROM t.username
                        OR s.display_name IS DISTINCT FROM t.display_name
                        OR s.top_role IS DISTINCT FROM t.top_role
                        OR s.current_status IS DISTINCT FROM t.current_status
                        OR s.roles IS DISTINCT FROM t.roles
                        OR s.num_roles IS DISTINCT FROM t.num_roles
                        OR s.boosting_since IS DISTINCT FROM t.boosting_since
                        OR s.permissions IS DISTINCT FROM t.permissions
                        OR s.screening_pending
                            IS DISTINCT FROM t.screening_pending
                      )
                    """
                )
                changed_existing_members_count = cursor.rowcount

            # ----------------------------------------------------------------
            # Process members who left.
            # Preserve the original leave timestamp semantics:
            # detection time using NOW() AT TIME ZONE 'UTC'.
            # ----------------------------------------------------------------
            left_list = sorted(left_ids)
            left_members_count = len(left_list)

            for batch in chunked(left_list):
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
                    WHERE discord_id = ANY(%s)
                    ON CONFLICT (discord_id) DO UPDATE SET
                        left_before = EXCLUDED.left_before
                    """,
                    (batch,),
                )

                cursor.execute(
                    "DELETE FROM server_members WHERE discord_id = ANY(%s)",
                    (batch,),
                )

            if left_list:
                execute_values(
                    cursor,
                    """
                    INSERT INTO member_events (
                        discord_id, event_type, event_datetime
                    )
                    VALUES %s
                    """,
                    [(member_id,) for member_id in left_list],
                    template="(%s, -1, NOW() AT TIME ZONE 'UTC')",
                    page_size=BATCH_SIZE,
                )

            # Read final counts inside the same transaction (one round trip),
            # then commit once.
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM server_members),
                    (SELECT COUNT(*) FROM former_members),
                    (SELECT COUNT(*) FROM member_events)
                """
            )
            server_count, former_count, events_count = cursor.fetchone()

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
            logging.info("Sync duration: %.2fs", time.perf_counter() - started_at)

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

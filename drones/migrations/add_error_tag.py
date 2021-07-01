import logging
from typing import Optional
import uuid

from tqdm import tqdm  # type: ignore

from spire.db import SessionLocal as session_local_spire  # type: ignore
from spire.journal.models import JournalEntry, JournalEntryTag  # type: ignore
from sqlalchemy import and_, or_  # type: ignore

logger = logging.getLogger(__name__)


def upgrade(journal_id: Optional[uuid.UUID], debug: bool = False) -> None:
    """
    Finds all entries that have a tag "type:error" but no matching tag of the form "error:.*",
    processes the error name from the entry title, and adds it as a tag "error:<error_name>".
    """
    old_level = logger.level
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Running upgrade for journal_id={journal_id}")

    db_session_spire = session_local_spire()
    try:
        """
        :old_entries request:

        SELECT journal_entries.id AS journal_entries_id,
            journal_entries.title AS journal_entries_title,
            journal_entries.created_at AS journal_entries_created_at,
            journal_entries.updated_at AS journal_entries_updated_at
        FROM journal_entries
        WHERE (
                EXISTS (
                    SELECT 1
                    FROM journal_entry_tags
                    WHERE journal_entry_tags.journal_entry_id = journal_entries.id
                        AND journal_entry_tags.tag = %(tag_1) s
                )
            )
            AND NOT (
                EXISTS (
                    SELECT 1
                    FROM journal_entry_tags
                    WHERE journal_entry_tags.journal_entry_id = journal_entries.id
                        AND journal_entry_tags.tag LIKE %(tag_2) s
                )
            )
            AND journal_entries.journal_id = %(journal_id_1) s
        """

        old_entries = db_session_spire.query(
            JournalEntry.id,
            JournalEntry.title,
            JournalEntry.created_at,
            JournalEntry.updated_at,
        ).filter(
            and_(
                db_session_spire.query(JournalEntryTag)
                .filter(JournalEntryTag.journal_entry_id == JournalEntry.id)
                .filter(JournalEntryTag.tag == "type:error")
                .exists(),
                ~db_session_spire.query(JournalEntryTag)
                .filter(JournalEntryTag.journal_entry_id == JournalEntry.id)
                .filter(JournalEntryTag.tag.like(f"error:%"))
                .exists(),
            )
        )

        if journal_id is not None:
            old_entries = old_entries.filter(JournalEntry.journal_id == journal_id)

        if debug:
            logger.debug("Query to retrieve old entries which need to be upgraded")
            logger.debug(old_entries)

        new_tags = []
        for entry_id, title, created_at, updated_at in tqdm(old_entries):
            title_parts = title.split(" - ")
            if len(title_parts) < 2:
                continue

            error_tag = f"error:{title_parts[1]}"

            new_tags.extend(
                [
                    JournalEntryTag(
                        journal_entry_id=entry_id,
                        tag=error_tag,
                        created_at=created_at,
                        updated_at=updated_at,
                    ),
                    JournalEntryTag(
                        journal_entry_id=entry_id, tag="error_migration_v1"
                    ),
                ]
            )

        db_session_spire.bulk_save_objects(new_tags)
        db_session_spire.commit()
        logger.info("Done!")
    except Exception as err:
        logger.error(f"Error in add_error_tag upgrade:{err}")
    finally:
        db_session_spire.close()

    logger.setLevel(old_level)


def downgrade(journal_id: Optional[uuid.UUID], debug: bool = False) -> None:
    """
    Remove errors tag from all entries wich containe tag error_migration_v1

    Selection (we run DELETE FROM with the same selection):
    SELECT journal_entry_tags.id AS journal_entry_tags_id,
        journal_entry_tags.journal_entry_id AS journal_entry_tags_journal_entry_id,
        journal_entry_tags.tag AS journal_entry_tags_tag,
        journal_entry_tags.created_at AS journal_entry_tags_created_at,
        journal_entry_tags.updated_at AS journal_entry_tags_updated_at
    FROM journal_entry_tags
    WHERE journal_entry_tags.journal_entry_id IN (
            SELECT journal_entries.id AS entry_id
            FROM journal_entries
            WHERE (
                    EXISTS (
                        SELECT 1
                        FROM journal_entry_tags
                        WHERE journal_entry_tags.journal_entry_id = journal_entries.id
                            AND journal_entry_tags.tag = %(tag_1) s
                    )
                )
                AND journal_entries.journal_id = %(journal_id_1) s
        )
        AND (
            journal_entry_tags.tag = %(tag_2) s
            OR journal_entry_tags.tag LIKE %(tag_3) s
        )
    """
    old_level = logger.level
    if debug:
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Running downgrade for journal_id={journal_id}")

    db_session_spire = session_local_spire()
    try:
        old_entries = db_session_spire.query(JournalEntry.id.label("entry_id")).filter(
            db_session_spire.query(JournalEntryTag)
            .filter(JournalEntryTag.journal_entry_id == JournalEntry.id)
            .filter(JournalEntryTag.tag == "error_migration_v1")
            .exists()
        )

        if journal_id is not None:
            old_entries = old_entries.filter(JournalEntry.journal_id == journal_id)

        # delete tags
        delete_query = db_session_spire.query(JournalEntryTag).filter(
            and_(
                JournalEntryTag.journal_entry_id.in_(
                    old_entries.subquery(name="entries_to_downgrade")
                ),
                or_(
                    JournalEntryTag.tag == "error_migration_v1",
                    JournalEntryTag.tag.like(f"error:%"),
                ),
            )
        )

        if debug:
            logger.debug("Query to retrieve entries which need to be downgraded")
            logger.debug(delete_query)

        num_deleted = delete_query.delete(synchronize_session="fetch")

        logger.info(f"Deleted {num_deleted} tags")

        db_session_spire.commit()

    except Exception as err:
        logger.error(f"Error in add_error_tag upgrade:{err}")
    finally:
        db_session_spire.close()

    logger.setLevel(old_level)

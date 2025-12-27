#!/usr/bin/env python3
"""
Clean up task instances marked as 'removed' for a specific DAG.

This is useful when DAG structure changes and old task instances
no longer match the new DAG definition.
"""

import sys
from airflow.models import TaskInstance
from airflow.utils.session import provide_session
from sqlalchemy import and_


@provide_session
def cleanup_removed_tasks(dag_id: str, session=None):
    """Delete task instances marked as 'removed' for the specified DAG."""
    # Find all removed task instances for the DAG
    removed_tis = (
        session.query(TaskInstance)
        .filter(
            and_(
                TaskInstance.dag_id == dag_id,
                TaskInstance.state == "removed",
            )
        )
        .all()
    )

    if not removed_tis:
        print(f"No removed task instances found for DAG '{dag_id}'")
        return 0

    print(f"Found {len(removed_tis)} removed task instances for DAG '{dag_id}':")
    for ti in removed_tis[:10]:  # Show first 10
        print(f"  - {ti.task_id} (run_id: {ti.run_id}, try_number: {ti.try_number})")
    if len(removed_tis) > 10:
        print(f"  ... and {len(removed_tis) - 10} more")

    # Delete them
    count = len(removed_tis)
    for ti in removed_tis:
        session.delete(ti)
    session.commit()

    print(f"\n✓ Deleted {count} removed task instances")
    return count


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: cleanup_removed_tasks.py <dag_id>")
        print("Example: cleanup_removed_tasks.py sec_scraper")
        sys.exit(1)

    dag_id = sys.argv[1]
    cleanup_removed_tasks(dag_id)


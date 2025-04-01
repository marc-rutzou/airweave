"""Pubsub for sync jobs."""

import asyncio
from typing import Optional
from uuid import UUID
from airweave.core.logging import logger

from pydantic import BaseModel

from airweave.db.session import get_db_context  
from airweave import crud
from airweave.schemas.sync_job import SyncJobUpdate
from airweave.core.shared_models import SyncJobStatus
from datetime import datetime
from airweave.core.config import settings

class SyncProgressUpdate(BaseModel):
    """Sync progress update data structure."""

    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    already_sync: int = 0
    is_complete: bool = False  # Add completion flag
    is_failed: bool = False  # Add failure flag


class SyncJobTopic:
    """Represents an active sync job's message stream."""

    def __init__(self, job_id: UUID):
        """Initialize topic."""
        self.job_id = job_id
        self.queues: list[asyncio.Queue] = []
        self.latest_update: Optional[SyncProgressUpdate] = None

    async def publish(self, update: SyncProgressUpdate) -> None:
        """Publish an update to all subscribers."""
        self.latest_update = update
        logger.info(f"\nPublishing update with is_complete={update.is_complete} to {len(self.queues)} subscribers\n")
        for queue in self.queues:
            await queue.put(update)
        logger.info(f"\nUpdate published to all subscribers\n")

    async def add_subscriber(self) -> asyncio.Queue:
        """Add a new subscriber and send them the latest update if available."""
        queue = asyncio.Queue()
        self.queues.append(queue)
        if self.latest_update:
            await queue.put(self.latest_update)
        return queue

    def remove_subscriber(self, queue: asyncio.Queue) -> None:
        """Remove a subscriber."""
        if queue in self.queues:
            self.queues.remove(queue)


class SyncPubSub:
    """Manages sync job topics and their subscribers."""

    def __init__(self) -> None:
        """Initialize the SyncPubSub instance."""
        self.topics: dict[UUID, SyncJobTopic] = {}

    def get_or_create_topic(self, job_id: UUID) -> SyncJobTopic:
        """Get an existing topic or create a new one."""
        if job_id not in self.topics:
            self.topics[job_id] = SyncJobTopic(job_id)
        return self.topics[job_id]

    def remove_topic(self, job_id: UUID) -> None:
        """Remove a topic when sync is complete."""
        if job_id in self.topics:
            del self.topics[job_id]

    async def publish(self, job_id: UUID, update: SyncProgressUpdate) -> None:
        """Publish an update to a specific job topic."""
        logger.info(f"Publishing update for job {job_id}, is_complete={update.is_complete}")
        topic = self.get_or_create_topic(job_id)
        await topic.publish(update)
        # If the update indicates completion, schedule topic removal
        if update.is_complete:
            logger.info(f"Update indicates completion, removing topic for job {job_id}")
            self.remove_topic(job_id)
        else:
            logger.info(f"Update does not indicate completion for job {job_id}")

    async def subscribe(self, job_id: UUID) -> asyncio.Queue:
        """Subscribe to a job's updates, creating the topic if it doesn't exist."""
        topic = self.get_or_create_topic(job_id)
        return await topic.add_subscriber()

    def unsubscribe(self, job_id: UUID, queue: asyncio.Queue) -> None:
        """Remove a subscriber from a topic."""
        if job_id in self.topics:
            self.topics[job_id].remove_subscriber(queue)


PUBLISH_THRESHOLD = 5


class SyncProgress:
    """Tracks sync progress and automatically publishes updates."""

    def __init__(self, job_id: UUID):
        """Initialize the SyncProgress instance."""
        self.job_id = job_id
        self.stats = SyncProgressUpdate()
        self._last_published = 0
        self._publish_threshold = PUBLISH_THRESHOLD

    def __getattr__(self, name: str) -> int:
        """Get counter value for any stat."""
        return getattr(self.stats, name)

    async def increment(self, stat_name: str, amount: int = 1) -> None:
        """Increment a counter and trigger update if threshold reached."""
        current_value = getattr(self.stats, stat_name, 0)
        setattr(self.stats, stat_name, current_value + amount)

        total_ops = sum(
            [self.stats.inserted, self.stats.updated, self.stats.deleted, self.stats.already_sync]
        )

        if total_ops - self._last_published >= self._publish_threshold:
            await self._publish()
            self._last_published = total_ops

    async def _publish(self) -> None:
        """Publish current progress."""
        await sync_pubsub.publish(self.job_id, self.stats)

    async def finalize(self, is_complete: bool = True, error: str = None) -> None:
        """Publish final progress and update database status."""
        
        logger.info(f"SyncProgress.finalize called for job {self.job_id} with is_complete={is_complete}")
        
        # Update the database record
        async with get_db_context() as db:
            try:
                # Get the existing job
                job = await crud.sync_job.get(db, id=self.job_id)
                if job:
                    # Get the superuser for system operations
                    system_user = await crud.user.get_by_email(db, email=settings.FIRST_SUPERUSER)
                    
                    # Create timestamp without timezone info for database compatibility
                    current_time = datetime.now()
                    
                    # Create the update object
                    job_update = SyncJobUpdate(
                        status=SyncJobStatus.COMPLETED if is_complete else SyncJobStatus.FAILED,
                        records_created=self.stats.inserted,
                        records_updated=self.stats.updated,
                        records_deleted=self.stats.deleted,
                        error=error,
                        completed_at=current_time if is_complete else None,
                        failed_at=None if is_complete else current_time,
                    )
                    
                    # Update the job in the database using the system user
                    await crud.sync_job.update(
                        db=db, 
                        db_obj=job, 
                        obj_in=job_update, 
                        current_user=system_user
                    )
                    logger.info(f"Updated database status for job {self.job_id} to "
                              f"{'COMPLETED' if is_complete else 'FAILED'}")
                else:
                    logger.error(f"Could not find job {self.job_id} in database")
            except Exception as e:
                logger.error(f"Error updating job status: {str(e)}")
        
        # Update the SSE status
        self.stats.is_complete = is_complete
        if error:
            self.stats.is_failed = True
        
        logger.info(f"About to publish final update for job {self.job_id}")
        await self._publish()
        logger.info(f"Final update published for job {self.job_id}")


# Create a global instance for the entire app
sync_pubsub = SyncPubSub()

"""CRUD operations for DAG models."""

from typing import Optional
from uuid import UUID

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from airweave import schemas
from airweave.core.logging import logger
from airweave.crud._base import CRUDBase
from airweave.db.unit_of_work import UnitOfWork
from airweave.models.dag import (
    DagEdge,
    DagNode,
    SyncDag,
)
from airweave.schemas.dag import (
    SyncDagCreate,
    SyncDagUpdate,
)


class CRUDSyncDag(CRUDBase[SyncDag, SyncDagCreate, SyncDagUpdate]):
    """CRUD operations for SyncDag."""

    async def create_with_nodes_and_edges(
        self,
        db: AsyncSession,
        *,
        obj_in: SyncDagCreate,
        current_user: schemas.User,
        uow: Optional[UnitOfWork] = None,
    ) -> SyncDag:
        """Create a DAG with its nodes and edges."""
        # Create the base DAG object
        db_obj = SyncDag(
            name=obj_in.name,
            description=obj_in.description,
            sync_id=obj_in.sync_id,
            organization_id=current_user.organization_id,
            created_by_email=current_user.email,
            modified_by_email=current_user.email,
        )
        db.add(db_obj)
        await db.flush()  # Flush to get the ID

        # Create nodes
        for node_in in obj_in.nodes:
            node_data = node_in.model_dump()
            db_node = DagNode(
                **node_data,
                dag_id=db_obj.id,
                organization_id=current_user.organization_id,
                created_by_email=current_user.email,
                modified_by_email=current_user.email,
            )
            db.add(db_node)
        await db.flush()

        # Create edges
        for edge_in in obj_in.edges:
            db_edge = DagEdge(
                from_node_id=edge_in.from_node_id,
                to_node_id=edge_in.to_node_id,
                dag_id=db_obj.id,
                organization_id=current_user.organization_id,
                created_by_email=current_user.email,
                modified_by_email=current_user.email,
            )
            db.add(db_edge)

        if not uow:
            await db.commit()
            await db.refresh(db_obj)

        # Reload with relationships
        result = await db.execute(
            select(SyncDag)
            .where(SyncDag.id == db_obj.id)
            .options(
                selectinload(SyncDag.nodes),
                selectinload(SyncDag.edges),
            )
        )
        return result.scalar_one()

    async def update_with_nodes_and_edges(
        self,
        db: AsyncSession,
        *,
        db_obj: SyncDag,
        obj_in: SyncDagUpdate,
        current_user: schemas.User,
    ) -> SyncDag:
        """Update a DAG with its nodes and edges."""
        # Update DAG fields
        # Extract update data for simple fields
        update_data = obj_in.model_dump(exclude={"nodes", "edges"}, exclude_unset=True) 
        
        # Update simple fields using the base update method
        simple_update = SyncDagUpdate(**update_data)
        db_obj = await self.update(db, db_obj=db_obj, obj_in=simple_update, current_user=current_user)
        
        # Handle nodes separately
        if obj_in.nodes is not None:
            # Delete existing nodes (cascade will handle edges)
            await db.execute(delete(DagNode).where(DagNode.dag_id == db_obj.id))
            # Create new nodes
            for node_in in obj_in.nodes:
                node_data = node_in.model_dump()
                db_node = DagNode(
                    **node_data,
                    dag_id=db_obj.id,
                    organization_id=current_user.organization_id,
                    created_by_email=current_user.email,
                    modified_by_email=current_user.email,
                )
                db.add(db_node)
            await db.flush()

        # If edges provided, replace all edges
        if obj_in.edges is not None:
            # Delete existing edges
            await db.execute(delete(DagEdge).where(DagEdge.dag_id == db_obj.id))
            # Create new edges
            for edge_in in obj_in.edges:
                edge_data = edge_in.model_dump()
                db_edge = DagEdge(
                    from_node_id=edge_data["from_node_id"],
                    to_node_id=edge_data["to_node_id"],
                    dag_id=db_obj.id,
                    organization_id=current_user.organization_id,
                    created_by_email=current_user.email,
                    modified_by_email=current_user.email,
                )
                db.add(db_edge)

        await db.commit()
        await db.refresh(db_obj)
        
        # Reload with relationships
        result = await db.execute(
            select(SyncDag)
            .where(SyncDag.id == db_obj.id)
            .options(
                selectinload(SyncDag.nodes),
                selectinload(SyncDag.edges),
            )
        )
        return result.scalar_one()

    async def get_by_sync_id(
        self,
        db: AsyncSession,
        *,
        sync_id: UUID,
        current_user: schemas.User,
    ) -> Optional[SyncDag]:
        """Get a DAG by sync ID."""
        result = await db.execute(
            select(SyncDag)
            .where(
                SyncDag.sync_id == sync_id,
                SyncDag.organization_id == current_user.organization_id,
            )
            .options(
                selectinload(SyncDag.nodes),
                selectinload(SyncDag.edges),
            )
        )
        return result.scalar_one_or_none()


sync_dag = CRUDSyncDag(SyncDag)

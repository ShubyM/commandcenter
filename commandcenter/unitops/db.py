import logging
from typing import Any, Dict, List, Tuple

from motor.motor_asyncio import AsyncIOMotorCollection

from commandcenter.unitops.models import UnitOp



_LOGGER = logging.getLogger("commandcenter.unitop")


async def find_unitop(
    collection: AsyncIOMotorCollection,
    unitop_id: str | None = None,
    query: Dict[str, Any] | None = None
) -> UnitOp | None:
    """Find one unitop."""
    if unitop_id is None and query is None:
        raise ValueError("No filters specified.")
    
    if unitop_id:
        document = await collection.find_one({"unitop_id": unitop_id})
    else:
        document = await collection.find_one(query)
    if document is not None:
        return UnitOp.parse_raw(document)

    _LOGGER.debug("Query returned 0 results", extra={"unitop_id": unitop_id, "query": query})


async def find_unitops(
    collection: AsyncIOMotorCollection,
    query: Dict[str, Any]
) -> List[UnitOp] | None:
    """Find multiple unitops."""
    documents = await collection.find(query).to_list()
    if documents:
        return [UnitOp.parse_raw(document) for document in documents]

    _LOGGER.debug("Query returned 0 results", extra={"query": query})


async def update_unitop(
    collection: AsyncIOMotorCollection,
    unitop: UnitOp
) -> Tuple[int, int]:
    """Update one unitop. Performs an upsert if unitop does not exist."""
    result = await collection.update_one(
        {"unitop_id": unitop.unitop_id},
        unitop.dict(),
        upsert=True
    )
    return result.matched_count, result.modified_count


async def delete_unitop(
    collection: AsyncIOMotorCollection,
    unitop_id: str | None = None,
    query: Dict[str, Any] | None = None
) -> int:
    """Delete one unitop."""
    if unitop_id is None and query is None:
        raise ValueError("No filters specified.")
    
    if unitop_id:
        result = await collection.delete_one({"unitop_id": unitop_id})
    else:
        result = await collection.delte_one(query)
    
    return result.deleted_count


async def delete_unitops(
    collection: AsyncIOMotorCollection,
    query: Dict[str, Any]
) -> int:
    """Delete multiple unitops."""
    result = await collection.delete_many(query)
    return result.deleted_count
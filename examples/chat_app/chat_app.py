"""
Simple async chat application demonstrating py-key-value with PydanticAdapter.

This example shows how to:
- Use PydanticAdapter for type-safe message storage
- Apply StatisticsWrapper to track message metrics
- Use TTLClampWrapper for automatic message expiration
- Use LoggingWrapper for operation debugging
"""

import asyncio
import logging
from datetime import datetime, timezone

from key_value.aio.adapters.pydantic import PydanticAdapter
from key_value.aio.stores.memory.store import MemoryStore
from key_value.aio.wrappers.logging.wrapper import LoggingWrapper
from key_value.aio.wrappers.statistics.wrapper import StatisticsWrapper
from key_value.aio.wrappers.ttl_clamp.wrapper import TTLClampWrapper
from pydantic import BaseModel


class ChatMessage(BaseModel):
    """A chat message with sender, content, and timestamp."""

    sender: str
    content: str
    timestamp: datetime


class ChatApp:
    """
    Simple chat application using py-key-value for message storage.

    Messages are stored with automatic expiration (24 hours max) and
    operation statistics tracking.
    """

    def __init__(self):
        # Base store: MemoryStore for fast in-memory storage
        base_store = MemoryStore()

        # Wrapper stack (applied inside-out):
        # 1. StatisticsWrapper - Track operation metrics
        # 2. TTLClampWrapper - Enforce TTL between 1 hour and 24 hours
        # 3. LoggingWrapper - Log all operations for debugging
        stats = StatisticsWrapper(key_value=base_store)
        ttl_clamped = TTLClampWrapper(key_value=stats, min_ttl=3600, max_ttl=86400)  # 1 hour min, 24 hours max
        wrapped_store = LoggingWrapper(key_value=ttl_clamped)

        # PydanticAdapter for type-safe message storage/retrieval
        self.adapter: PydanticAdapter[ChatMessage] = PydanticAdapter[ChatMessage](
            key_value=wrapped_store,
            pydantic_model=ChatMessage,
        )

        # Store reference to statistics wrapper for metrics
        self.stats_wrapper = stats

    async def send_message(self, conversation_id: str, sender: str, content: str) -> str:
        """
        Send a message to a conversation.

        Args:
            conversation_id: Unique identifier for the conversation
            sender: Username of the message sender
            content: Message content

        Returns:
            Message ID (timestamp-based)
        """
        message = ChatMessage(sender=sender, content=content, timestamp=datetime.now(tz=timezone.utc))

        # Use timestamp as message ID for chronological ordering
        message_id = message.timestamp.isoformat()

        # Store message with 24-hour TTL (will be clamped by TTLClampWrapper)
        await self.adapter.put(
            collection=f"conversation:{conversation_id}",
            key=message_id,
            value=message,
            ttl=86400,  # 24 hours
        )

        return message_id

    async def get_message(self, conversation_id: str, message_id: str) -> ChatMessage | None:
        """
        Retrieve a specific message from a conversation.

        Args:
            conversation_id: Unique identifier for the conversation
            message_id: Message identifier (timestamp)

        Returns:
            ChatMessage if found, None otherwise
        """
        return await self.adapter.get(collection=f"conversation:{conversation_id}", key=message_id)

    async def delete_message(self, conversation_id: str, message_id: str) -> bool:
        """
        Delete a message from a conversation.

        Args:
            conversation_id: Unique identifier for the conversation
            message_id: Message identifier (timestamp)

        Returns:
            True if message was deleted, False if not found
        """
        return await self.adapter.delete(collection=f"conversation:{conversation_id}", key=message_id)

    def get_statistics(self) -> dict[str, int]:
        """
        Get operation statistics across all conversations.

        Returns:
            Dictionary with aggregated operation counts (puts, gets, deletes, etc.)
        """
        if isinstance(self.stats_wrapper, StatisticsWrapper):
            # Aggregate statistics across all collections (conversations)
            total_puts = sum(coll_stats.put.count for coll_stats in self.stats_wrapper.statistics.collections.values())
            total_gets = sum(coll_stats.get.count for coll_stats in self.stats_wrapper.statistics.collections.values())
            total_deletes = sum(coll_stats.delete.count for coll_stats in self.stats_wrapper.statistics.collections.values())
            get_hits = sum(coll_stats.get.hit for coll_stats in self.stats_wrapper.statistics.collections.values())
            get_misses = sum(coll_stats.get.miss for coll_stats in self.stats_wrapper.statistics.collections.values())

            return {
                "total_puts": total_puts,
                "total_gets": total_gets,
                "total_deletes": total_deletes,
                "get_hits": get_hits,
                "get_misses": get_misses,
            }
        return {}


async def main():
    """Demonstrate the chat application."""
    # Configure logging for the demo
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    chat = ChatApp()

    # Send some messages
    msg1_id = await chat.send_message("conv-123", "Alice", "Hello, Bob!")
    msg2_id = await chat.send_message("conv-123", "Bob", "Hi Alice, how are you?")
    msg3_id = await chat.send_message("conv-123", "Alice", "I'm doing great, thanks!")

    print("Messages sent:")
    print(f"  Message 1 ID: {msg1_id}")
    print(f"  Message 2 ID: {msg2_id}")
    print(f"  Message 3 ID: {msg3_id}")

    # Retrieve messages
    print("\nRetrieving messages:")
    message1 = await chat.get_message("conv-123", msg1_id)
    if message1:
        print(f"  {message1.sender}: {message1.content} (at {message1.timestamp})")

    message2 = await chat.get_message("conv-123", msg2_id)
    if message2:
        print(f"  {message2.sender}: {message2.content} (at {message2.timestamp})")

    message3 = await chat.get_message("conv-123", msg3_id)
    if message3:
        print(f"  {message3.sender}: {message3.content} (at {message3.timestamp})")

    # Delete a message
    print(f"\nDeleting message: {msg2_id}")
    deleted = await chat.delete_message("conv-123", msg2_id)
    print(f"  Deleted: {deleted}")

    # Try to retrieve deleted message
    deleted_msg = await chat.get_message("conv-123", msg2_id)
    print(f"  Retrieved after delete: {deleted_msg}")

    # Show statistics
    print("\nOperation Statistics:")
    stats = chat.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    asyncio.run(main())

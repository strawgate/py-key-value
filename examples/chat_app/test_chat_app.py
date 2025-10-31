"""Tests for the chat application example."""

import pytest

from chat_app import ChatApp, ChatMessage


class TestChatApp:
    """Test suite for the ChatApp example."""

    @pytest.fixture
    async def chat_app(self) -> ChatApp:
        """Create a ChatApp instance for testing."""
        return ChatApp()

    async def test_send_and_retrieve_message(self, chat_app: ChatApp):
        """Test sending and retrieving a single message."""
        # Send a message
        message_id = await chat_app.send_message("test-conv", "Alice", "Hello, world!")

        # Retrieve the message
        message = await chat_app.get_message("test-conv", message_id)

        assert message is not None
        assert isinstance(message, ChatMessage)
        assert message.sender == "Alice"
        assert message.content == "Hello, world!"
        assert message.timestamp is not None

    async def test_send_multiple_messages(self, chat_app: ChatApp):
        """Test sending multiple messages to the same conversation."""
        # Send multiple messages
        msg1_id = await chat_app.send_message("conv-1", "Alice", "First message")
        msg2_id = await chat_app.send_message("conv-1", "Bob", "Second message")
        msg3_id = await chat_app.send_message("conv-1", "Alice", "Third message")

        # Retrieve all messages
        msg1 = await chat_app.get_message("conv-1", msg1_id)
        msg2 = await chat_app.get_message("conv-1", msg2_id)
        msg3 = await chat_app.get_message("conv-1", msg3_id)

        assert msg1 is not None
        assert msg1.sender == "Alice"
        assert msg1.content == "First message"

        assert msg2 is not None
        assert msg2.sender == "Bob"
        assert msg2.content == "Second message"

        assert msg3 is not None
        assert msg3.sender == "Alice"
        assert msg3.content == "Third message"

    async def test_delete_message(self, chat_app: ChatApp):
        """Test deleting a message."""
        # Send a message
        message_id = await chat_app.send_message("conv-2", "Charlie", "Temporary message")

        # Verify it exists
        message = await chat_app.get_message("conv-2", message_id)
        assert message is not None

        # Delete the message
        deleted = await chat_app.delete_message("conv-2", message_id)
        assert deleted is True

        # Verify it's gone
        message = await chat_app.get_message("conv-2", message_id)
        assert message is None

    async def test_delete_nonexistent_message(self, chat_app: ChatApp):
        """Test deleting a message that doesn't exist."""
        deleted = await chat_app.delete_message("conv-3", "nonexistent-id")
        assert deleted is False

    async def test_retrieve_nonexistent_message(self, chat_app: ChatApp):
        """Test retrieving a message that doesn't exist."""
        message = await chat_app.get_message("conv-4", "nonexistent-id")
        assert message is None

    async def test_conversation_isolation(self, chat_app: ChatApp):
        """Test that messages are isolated between conversations."""
        # Send messages to different conversations
        msg1_id = await chat_app.send_message("conv-a", "Alice", "Message in conv-a")
        msg2_id = await chat_app.send_message("conv-b", "Bob", "Message in conv-b")

        # Verify messages are in their respective conversations
        msg1_in_a = await chat_app.get_message("conv-a", msg1_id)
        msg1_in_b = await chat_app.get_message("conv-b", msg1_id)

        assert msg1_in_a is not None
        assert msg1_in_a.content == "Message in conv-a"
        assert msg1_in_b is None  # Should not be in conv-b

        msg2_in_b = await chat_app.get_message("conv-b", msg2_id)
        msg2_in_a = await chat_app.get_message("conv-a", msg2_id)

        assert msg2_in_b is not None
        assert msg2_in_b.content == "Message in conv-b"
        assert msg2_in_a is None  # Should not be in conv-a

    async def test_statistics_tracking(self, chat_app: ChatApp):
        """Test that statistics are properly tracked."""
        # Perform some operations
        msg_id = await chat_app.send_message("stats-conv", "Alice", "Test message")
        await chat_app.get_message("stats-conv", msg_id)
        await chat_app.get_message("stats-conv", "nonexistent")
        await chat_app.delete_message("stats-conv", msg_id)

        # Check statistics
        stats = chat_app.get_statistics()

        assert stats["total_puts"] >= 1  # At least one put
        assert stats["total_gets"] >= 2  # At least two gets
        assert stats["total_deletes"] >= 1  # At least one delete
        assert stats["get_hits"] >= 1  # At least one hit
        assert stats["get_misses"] >= 1  # At least one miss

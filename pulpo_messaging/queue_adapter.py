from .message import Message


class QueueAdapter():

    def enqueue(self, message: Message) -> Message:
        pass

    def dequeue(self) -> Message:
        pass

    def commit(self, message: Message, is_success: bool = True) -> Message:
        pass

    def rollback(self, message: Message) -> Message:
        pass

    def peek(self, message_id: str) -> Message:
        pass

    def delete(self, message_id: str):
        pass

from kessel.message import Message


class QueueAdapter():

    def enqueue(self, message: Message) -> Message:
        pass

    def dequeue(self) -> Message:
        pass

    def commit(self, message: Message) -> Message:
        pass

    def rollback(self, message: Message) -> Message:
        pass

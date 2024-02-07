
from collections import deque

from linguacrawl.link import Link

class PendingURLsQueue(object):

    def static_method_init():
        return PendingURLsQueue()

    def __init__(self):
        self.pending_urls = deque(maxlen=None)

    def __len__(self):
        return len(self.pending_urls)

    def __iter__(self):
        yield from self.pending_urls

    def __contains__(self, key):
        return key in self.pending_urls

    def append(self, v):
        if not isinstance(v, Link):
            raise Exception(f"Link instance was expected, but got {type(v)}")

        self.pending_urls.append(v)

    def pop(self):
        return self.pending_urls.popleft()

    def extend(self, l):
        self.pending_urls.extendleft(reversed(l))

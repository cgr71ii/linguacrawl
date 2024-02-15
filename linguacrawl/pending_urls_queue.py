
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

    def get_status_object(self):
        targets = []

        for u in self.pending_urls:
            targets.append(u.get_norm_url())

        return {'visited': self.visited, 'pendingurls': targets, 'attempts': self.attempts}

    def load_status(self, status_obj):
        visited = status_obj['visited']

        for u in status_obj['pendingurls']:
            self.pending_urls.append(Link(u))

        attempts = status_obj['attempts']

        return {"visited": visited, "attempts": attempts}

    def document_downloaded(self, doc):
        # After self.url_processed
        pass

    def url_processed(self, url):
        # Before self.document_downloaded
        pass

import os
import heapq

class PendingURLsQueueLang(PendingURLsQueue):

    def static_method_init():
        return PendingURLsQueueLang()

    def __init__(self):
        self.pending_urls = []
        self.seen_urls = set() # URLs which are or have been in the queue
        self.processed_urls = set() # URLs which have been processed
        self.lang_priority_urls = {} # URL -> priority = {1, 2, 3} # 1 lang is target, 2 lang is unknown, 3 lang is not target
        self.do_not_update = set()
        self.counter = 0

        if "LINGUACRAWL_TARGET_LANGS" not in os.environ:
            raise Exception("Envvar not found: LINGUACRAWL_TARGET_LANGS")

        self.target_langs = os.environ["LINGUACRAWL_TARGET_LANGS"].strip().split(',')
        self.target_langs = list(filter(lambda lang: len(lang) == "2"), self.target_langs)

        if len(self.target_langs) == 0:
            raise Exception("Could not load any target language")

    def __iter__(self):
        for pending_url in sorted(self.pending_urls):
            yield pending_url[1]

    def __contains__(self, key):
        return key in map(lambda l: l[1], self.pending_urls)

    def get_priority(self, l, lang_priority=None):
        lang_priority = 2 if lang_priority is None else lang_priority
        priority = (lang_priority, self.counter, l)

        return priority

    def append(self, v):
        if not isinstance(v, Link):
            raise Exception(f"Link instance was expected, but got {type(v)}")

        if str(v) in self.processed_urls:
            # URL already processed by the frontier
            return

        lang_priority = self.lang_priority_urls[str(v)] if str(v) in self.lang_priority_urls else None
        seen_url = str(v) in self.seen_urls
        update = seen_url and lang_priority is not None and str(v) not in self.do_not_update

        if seen_url and not update:
            return
        if update:
            # TODO
            pass

        heapq.heappush(self.pending_urls, self.get_priority(v, lang_priority=lang_priority))
        self.seen_urls.add(str(v))

        self.counter += 1

    def pop(self):
        pop_url = heapq.heappop(self.pending_urls)

        return pop_url

    def extend(self, l):
        for v in l:
            self.append(v)

    def get_status_object(self):
        # TODO
        pass

    def load_status(self, status_obj):
        # TODO
        pass

    def document_downloaded(self, doc):
        # Update links priority if needed
        if doc is None:
            return

        #doc_url = doc.url
        doc_lang = doc.get_lang()
        doc_children_urls = doc.get_link_set() if doc.utf_text else []
        doc_children_urls = [] if not isinstance(doc_children_urls, list) else doc_children_urls
        doc_children_urls_priority = 3 # priority 2 will be utilized for URLs which we do not know if should be prioritized (i.e., not lang detected yet)

        if isinstance(doc_lang, str) and len(doc_lang) == 2 and doc_lang in self.target_langs:
            doc_children_urls_priority = 1

        for doc_children_url in doc_children_urls:
            self.lang_priority_urls[str(doc_children_url)] = doc_children_urls_priority

            # append or update (if already downloaded, it will be ignored)
            self.append(doc_children_url)
            self.do_not_update.add(doc_children_url)

    def url_processed(self, url):
        self.processed_urls.add(str(url))

#!/usr/bin/env python3
import sys

last_doc = None

for line in sys.stdin:
    doc_id, title = line.strip().split('\t', 1)
    if doc_id != last_doc:
        print(f"{doc_id}\t{title}")
        last_doc = doc_id

#!/usr/bin/env python3
import sys

for line in sys.stdin:
    try:
        doc_id, title, _ = line.strip().split('\t', 2)
        print(f"{doc_id}\t{title}")
    except:
        continue

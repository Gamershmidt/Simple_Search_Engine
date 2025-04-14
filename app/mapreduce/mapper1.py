#!/usr/bin/env python3
import sys
import re


for line in sys.stdin:
    try:
        doc_id, title, text = line.strip().split('\t', 2)
        words = re.findall(r'\w+', text.lower())
        for word in words:
            print(f"{word}\t{doc_id}\t1")
    except:
        continue
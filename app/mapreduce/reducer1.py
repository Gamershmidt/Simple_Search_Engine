#!/usr/bin/env python3
import sys

current_word = None
current_doc = None
current_count = 0

for line in sys.stdin:
    word, doc_id, count = line.strip().split('\t')
    count = int(count)
    
    if word == current_word and doc_id == current_doc:
        current_count += count
    else:
        if current_word and current_doc:
            print(f"{current_word}\t{current_doc}\t{current_count}")
        current_word = word
        current_doc = doc_id
        current_count = count

if current_word and current_doc:
    print(f"{current_word}\t{current_doc}\t{current_count}")
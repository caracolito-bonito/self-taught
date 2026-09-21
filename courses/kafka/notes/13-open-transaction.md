1. Why was offset 2 initially hidden even though it was nontransactional?
2. What does LSO = 1 mean: can offset 1 itself be returned?
3. How does the high watermark differ from the LSO?
4. What changed when we committed?
5. If we had aborted instead, which application records would read_committed return?

1. Because even nontransactional events are waiting for unresolved transaction in the same partition
2. LSO = 1 mean that Last stable offset is 1 so kafka can safely return only offsets below 1 for "read committed"
3. hw = before which offset partition is readable (the least exlusive offset between all in sync replicas), lso = additional boundary for read_commited.
4. commit resolves all outcomes and makes offset 1 and 2 readable.
5. i guess 0 and 2, because they both are non-transactional, so when we aborted transaction, offset 1 was filtered by "read-commited".

Self check:
1. What survived the crash?
2. What did not?
3. Why did O0 appear twice?
4. Why did the next successful restart receive O1?

1. Side effect survived the crash (so consumer processed the event)
2. Commit new offset didnt happen (we dropped app before commit intentionally)
3. Because after recovery we have no commited offset (and 0 was selected by `auto.offset.reset=earliest`), so when we processed the same event that was already processed when crash happened.
4. Processing O0 sucessfully and committing 1 caused the next restart to receive O1.

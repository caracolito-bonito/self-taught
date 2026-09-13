1. Why replaying offset 5 did not increment the counter.
2. Why offset 6 did increment it.
3. Why marker insertion and counter update must share a transaction.
4. Whether publishing the same business event at a new Kafka offset would be deduplicated by this design.

1. Because i check a marker (by trying to insert a row with the same key: topic + partition + offset). If marker exists (and we dont insert anything) we dont process that message in "our system"
2. Beause there was no marker for offset 6
3. Yes because this operation should be atomic or we can corrupt the data in our system if something happens between marker insertion and incrementing
4. No, because a new offset will create a different marker so we will process it, to deduplicate in such way we need some stable business event ID.

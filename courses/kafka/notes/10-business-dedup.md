1. Why did offsets 7 and 8 represent the same business event?
2. Why wouldn’t (topic, partition, offset) deduplicate them?
3. Why use event_id rather than order_id as the database marker?
4. What do inserted-row counts 1 and 0 tell the consumer to do?
5. Why must the marker and business effect share a transaction?
6. What happens if a producer generates a new event ID for every retry?
7. Does this in-memory version remember processed events after restarting? Why?

1. Because each event_id represents unique event in our app, and offset 7 and 8 have the same one
2. Because we have 2 similar records (from the business perspective) but they have different offset
3. If we choose order_id we could deduplicate what we dont want to deduplicate (like different event types for the same order)
4. It tells: if 1 -> apply business effect, if 0 -> skip the duplicate effect
5. Because if some problem happens we need to rollback both marker and sideffect to recover correctly
6. We will process this event like new one
7. No, because it isn't a persistent DB. After restart nothing is stored. To remember we need to use persistent DB (like file SQLite).

1. What does successful delivery mean inside an open transaction?
2. Does aborting physically remove the records?
3. How do read_uncommitted and read_committed differ?
4. Why did the consumer skip offsets 0–1 but return 3–4?
5. Does this Kafka transaction also make an external database update atomic?

1. Succesfull delivery means delivery, but doesnt mean visibility. Because visibility depends on trasaction outcome.
2. No, it makes them invisible for consumers under some isolation level (read_commited)
3. read_uncommited reads all records(open, commited and aborted transactions), read_commited filter aborted and waits open transactions (also reads nontransactional records)
4. because 0-1 was occupied by records from aborted transaction
5. No, because it's only kafka level transaction

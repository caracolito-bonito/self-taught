1. Why was the input read again after the crash?
2. Why didn’t read_committed return the first attempt’s output?
3. Why did we stage input offset 1, not 0?
4. What two things did commit_transaction commit together?
5. Why would calling consumer.commit_message separately reintroduce a failure window?
6. Would an HTTP request or database write inside this workflow also be covered by the Kafka transaction?

1. Input checkpoint (input topic) wasnt commited because we dropped in the middle of producer transaction which should have commited that effect
2. first attempt output is part of cancelled transaction, so it was filtered out
3. because 1st offset is after processed 0 offset
4. input offset and the entry for output topic (processed:order-42)
5. because it would be 2 separate actions: commiting new input checkpoint offset and our effect of producing new event
6. no

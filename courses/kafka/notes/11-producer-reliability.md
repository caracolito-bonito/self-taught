1. Why did two sends with idempotence enabled produce two offsets?
2. Which retries does producer idempotence handle?
3. How do queue timeout and delivery timeout differ?
4. Why did this experiment take about ten seconds?
5. Why doesn’t a delivery timeout generally prove that Kafka stored nothing?


1. Because idempotence setting doesnt mean "business key deduplication", it means that deduplication in internal retries of the same message
2. Internal
3. Queue timeout - internal queue, how many is given to wait for free space in internal queue (if it's full). Delivery timeout - time allowed for already enqueued record to reach a delivery result (with retries included).
4. Because kafka was switched off and the delivery timeout for producer is 10 secs
5. Because it doesnt mean reject, producer times out without knowing whether append succeded or not

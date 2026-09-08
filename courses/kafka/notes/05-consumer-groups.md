Self check.

1. How the two consumers divided three partitions.
2. What changed when one consumer stopped.
3. Why this experiment—with commits disabled and latest configured—does not prove reliable recovery after
    failure.

1. There are 2 consumers that devide 3 partitions in a format like:
C1 -> P0, P1
C2 -> P2
They receive all 4 records together.

2. Then 1 consumer was stopped, and after some delay partitions were rebalanced and C1 now owns all partitions:
C1 -> P0, P1, P2
After that all 4 recordes were received by C1.

3. With auto commit disabled, no manual commits, no previously committed offsets and `auto.offset.reset` latest, if we imagine some failure in the process of consuming, after recovery we dont have any commited offset and `latest` starts from the log end. So it failed in some "middle" and there were some unprocessed records, we wont process them after recovery.




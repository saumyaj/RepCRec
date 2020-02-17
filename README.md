# RepCRec
Replicated Concurrency Control and Recovery

This is a Java implementation of a database system multi-version concurrency control, deadlock detection, replication, and failure recovery. The database uses the available copies approach for replication using strict two-phase locking (using read and write locks) at each site and validation at commit time.

Reprozip has been used to trace the following call:
"java -jar RepCRec.jar sample_test1.txt sample_test2.txt sample_test3.txt sample_test4.txt sample_test5.txt"

Change logging level to see more details about flow

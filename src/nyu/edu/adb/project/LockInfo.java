package nyu.edu.adb.project;

import java.util.*;
import java.util.concurrent.locks.Lock;

public class LockInfo {
    private Optional<String> writeLockHolder;
    private List<String> readLockHolders;
    private Queue<String> transactionQueue;

    LockInfo() {
        readLockHolders = new ArrayList<>();
        transactionQueue = new LinkedList<>();
        writeLockHolder = Optional.empty();
    }

    public void setWriteLockHolder(String writeLockHolder) throws Exception {
        if (this.writeLockHolder.isPresent()) {
            throw new Exception("Transaction " + this.writeLockHolder.get() + " already holds a write lock");
        }
        this.writeLockHolder = Optional.of(writeLockHolder);
    }

    public void releaseWriteLock(String transactionName) throws Exception {
        if (this.writeLockHolder.isPresent() && this.writeLockHolder.get().equals(transactionName)) {
            this.writeLockHolder = Optional.empty();
        } else {
            throw new Exception("Illegal attempt to release write lock");
        }
    }

    public Optional<String> getWriteLockHolder() {
        return writeLockHolder;
    }

    public List<String> getReadLockHolders() {
        return readLockHolders;
    }

    public Queue<String> getTransactionQueue() {
        return transactionQueue;
    }
}

package nyu.edu.adb.project;

import java.util.HashSet;
import java.util.Set;

class LockTable {
    Set<String> writeLocks;
    Set<String> readLocks;

    LockTable() {
        writeLocks = new HashSet<>();
        readLocks = new HashSet<>();
    }

    public boolean addWriteLock(String variableName) {
        return writeLocks.add(variableName);
    }

    public boolean addReadLock(String variableName) {
        return readLocks.add(variableName);
    }

    public boolean releaseWriteLock(String variableName) {
        return writeLocks.remove(variableName);
    }

    public boolean releaseReadLock(String variableName) {
        return readLocks.remove(variableName);
    }
}

package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

class LockTable {
    Set<String> writeLocks;
    HashMap<String, Integer> readLockCount;

    LockTable() {
        writeLocks = new HashSet<>();
        readLockCount = new HashMap<>();
    }

    public boolean addWriteLock(String variableName) {
        return writeLocks.add(variableName);
    }

    public boolean addReadLock(String variableName) {
        if(readLockCount.containsKey(variableName)) {
            readLockCount.put(variableName, readLockCount.get(variableName) + 1);
        } else {
            readLockCount.put(variableName, 1);
        }
        return true;
    }

    public boolean releaseWriteLock(String variableName) {
        return writeLocks.remove(variableName);
    }

    public boolean releaseReadLock(String variableName) {
        int count = readLockCount.get(variableName);
        if(count == 1) {
            readLockCount.remove(variableName);
        } else {
            readLockCount.put(variableName, count - 1);
        }
        return true;
    }

    public void clearLockTable() {
        writeLocks = new HashSet<>();
        readLockCount = new HashMap<>();
    }
}
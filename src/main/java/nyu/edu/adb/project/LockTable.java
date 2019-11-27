package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class LockTable {
    Map<String, String> writeLocks;
    Map<String, Set<String>> readLocks;
    HashMap<String, Integer> readLockCount;

    LockTable() {
        writeLocks = new HashMap<>();
        readLocks = new HashMap<>();
        readLockCount = new HashMap<>();
    }

    public boolean addWriteLock(String variableName, String transactionId) {
        if (writeLocks.containsKey(variableName)
                && !writeLocks.get(variableName).equals(transactionId)) {
            return false;
        }

        // Return false if some other transaction is holding a read lock
        if (readLocks.containsKey(variableName)) {
            Set<String> readLockHolders = readLocks.get(variableName);
            for (String tr: readLockHolders) {
                if (!tr.equals(transactionId)) {
                    return false;
                }
            }
        }

        writeLocks.put(variableName, transactionId);
        return true;
    }

    public boolean addReadLock(String variableName, String transactionId) {
        // Return false some other transaction has write lock
        if (writeLocks.containsKey(variableName)
                && !writeLocks.get(variableName).equals(transactionId)) {
            return false;
        }

        // Add new transaction to the set of read lock holders
        Set<String> readLockHolders = readLocks.getOrDefault(variableName, new HashSet<>());
        readLockHolders.add(transactionId);
        readLocks.put(variableName, readLockHolders);

        if(readLockCount.containsKey(variableName)) {
            readLockCount.put(variableName, readLockCount.get(variableName) + 1);
        } else {
            readLockCount.put(variableName, 1);
        }
        return true;
    }

    public boolean releaseWriteLock(String variableName) {
        if (writeLocks.containsKey(variableName)) {
            writeLocks.remove(variableName);
            return true;
        }
        return false;
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
        writeLocks.clear();
        readLocks.clear();
        readLockCount = new HashMap<>();
    }
}
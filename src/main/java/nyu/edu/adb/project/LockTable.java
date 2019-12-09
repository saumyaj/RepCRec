package nyu.edu.adb.project;

import java.util.*;

class LockTable {
    private Map<String, String> writeLocks;
    private Map<String, Set<String>> readLocks;
    private HashMap<String, Integer> readLockCount;

    LockTable() {
        writeLocks = new HashMap<>();
        readLocks = new HashMap<>();
        readLockCount = new HashMap<>();
    }

    /**
     * Returns the transactionId of the transaction holding the write lock on the given variable
     * @param variableName name of the variable
     * @return TransactionId as a String of the writeLock holder transaction
     * @author Saumya
     */
    Optional<String> getWriteLockHolder(String variableName) {
        if (writeLocks.containsKey(variableName)) {
            return Optional.of(writeLocks.get(variableName));
        }
        return Optional.empty();
    }

    /**
     * Returns the List of transactionId of the transaction holding the read lock on the given variable
     * @param variableName name of the variable
     * @return List of transactionIds (Strings) having the read lock
     * @author Saumya
     */
    List<String> getReadLockHolders(String variableName) {
        List<String> readLockHolders = new ArrayList<>();
        readLockHolders.addAll(readLocks.getOrDefault(variableName, new HashSet<>()));
        return readLockHolders;
    }

    /**
     * Returns true if the given transaction can acquire a write lock on the given variable
     * @param variableName Name of the variable
     * @param transactionId id of the transaction
     * @return true if the given transaction can acquire a write lock on the given variable
     * @author Saumya
     */
    boolean isWriteLockAvailable(String variableName, String transactionId) {
        // False if some other transaction is holding the write lock
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

        return true;
    }

    /**
     * Sets the writeLockHolder for the given variable as the given transactionId
     * @param variableName name of the variable
     * @param transactionId id of the transaction
     * @return Returns true if the writeLockHolder was successfully set as the given transactionId
     * @author Saumya
     */
    boolean addWriteLock(String variableName, String transactionId) {

        if (!isWriteLockAvailable(variableName, transactionId)) {
            return false;
        }

        //remove the read lock if the current transaction is holding one in order to enforce promotion of locks
        Set<String> readLockHolders = readLocks.getOrDefault(variableName, new HashSet<>());
        if (readLockHolders.contains(transactionId)) {
            readLockHolders.remove(transactionId);
        }
        writeLocks.put(variableName, transactionId);
        return true;
    }

    /**
     * Returns true if the given transaction can acquire a readlock on the given variable
     * @param variableName name of the variable
     * @param transactionId id of the transaction
     * @return true if the given transaction can acquire a readlock on the given variable
     * @author Saumya
     */
    private boolean isReadLockAvailable(String variableName, String transactionId) {
        // Return false some other transaction has write lock
        if (writeLocks.containsKey(variableName)
                && !writeLocks.get(variableName).equals(transactionId)) {
            return false;
        }
        return true;
    }

    /**
     * Adds the given transaction to the list of read lock holders of the given variable
     * @param variableName name of the variable
     * @param transactionId id of the transaction
     * @return true if the given transaction was successfully added to the list of read lock holders
     * of the given variable
     * @author Saumya
     */
    boolean addReadLock(String variableName, String transactionId) {

        if (!isReadLockAvailable(variableName, transactionId)) {
            return false;
        }

        if (writeLocks.containsKey(variableName) && writeLocks.get(variableName).equals(transactionId)) {
            return true;
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

    /**
     * Removes the given variable from the set of writeLocked variables
     * @param variableName name of the variable
     * @return True if the variable is successfully removed from the set of writeLocked variables.
     * @author Saumya
     */
    boolean releaseWriteLock(String variableName) {
        if (writeLocks.containsKey(variableName)) {
            writeLocks.remove(variableName);
            return true;
        }
        return false;
    }

    /**
     * removes the given transactionId from the list of the readLockHolders of the given variable
     * @param variableName name of the variable
     * @param transactionName name of the transaction
     * @return true if the transaction name is removed from the list of readLock holders
     * @author Saumya
     */
    boolean releaseReadLock(String variableName, String transactionName) {

        if (!readLocks.containsKey(variableName)) {
            return true;
        }
        readLocks.get(variableName).remove(transactionName);

        int count = readLockCount.get(variableName);
        if(count == 1) {
            readLockCount.remove(variableName);
        } else {
            readLockCount.put(variableName, count - 1);
        }
        return true;
    }

    /**
     * clears the lockTable data. A helper method to simulate loss of lock info when the site goes down.
     * @author Saumya
     */
    void clearLockTable() {
        writeLocks.clear();
        readLocks.clear();
        readLockCount = new HashMap<>();
    }
}
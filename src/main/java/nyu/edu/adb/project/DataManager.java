package nyu.edu.adb.project;

import java.util.*;

class DataManager {
    private final LockTable lockTable;
    private final Map<String, Integer> dataMap;
    private final Map<String, Map<Long, Integer>> writeHistory;
    private final Set<String> unsafeVariablesForReading;

    DataManager() {
        lockTable = new LockTable();
        dataMap = new HashMap<>();
        unsafeVariablesForReading = new HashSet<>();
        writeHistory = new HashMap<>();
    }

    /**
     * Returns the last committed value of the given variable if available
     * @author Saumya
     */
    public Integer read(String variableName) {
        if (!dataMap.containsKey(variableName)) {
            throw new RuntimeException("Site does not contain variable");
        }
        return dataMap.get(variableName);
    }

    /**
     * Returns the last committed value of the given variable when the requesting transaction was started
     * @author Saumya
     */
    Optional<Integer> readForRO(String variableName, Long tickTime) {
        if (!dataMap.containsKey(variableName)) {
            throw new RuntimeException("Site does not contain variable");
        }
        Map<Long, Integer> variableHistory = writeHistory.get(variableName);
        if (variableHistory.containsKey(tickTime)) {
            return Optional.of(variableHistory.get(tickTime));
        }
        return Optional.empty();
    }

    /**
     * Writes the new value of the variable to the site
     * @author Saumya
     */
    public void write(String variableName, int val, long tickTime) {
        if (!dataMap.containsKey(variableName)) {
            throw new RuntimeException("Site does not contain variable");
        }
        dataMap.put(variableName, val);
        Map<Long, Integer> variableHistory = writeHistory.get(variableName);
        variableHistory.put(tickTime, val);

        if (unsafeVariablesForReading.contains(variableName)) {
            unsafeVariablesForReading.remove(variableName);
        }
    }

    /**
     * returns true if a fresh copy of the variable is available at this site
     * @author Saumya
     */
    boolean isVariableSafeForRead(String variableName) {
        return !unsafeVariablesForReading.contains(variableName);
    }

    /**
     * clears the set of stale state (unsafe to read) variables
     * @author Saumya
     */
    void clearStaleSet() {
        unsafeVariablesForReading.clear();
    }

    /**
     * Adds variable to the set of staleState variables. Thus marking the variable as unsafe to read
     * @author Saumya
     */
    void addVariableToStaleSet(String variableName) {
        if (dataMap.containsKey(variableName)) {
            unsafeVariablesForReading.add(variableName);
        } else {
            throw new RuntimeException("this variable is not present at this site");
        }
    }

    /**
     * @author Saumya
     */
    void initializeVar(String variableName, int val) {
        dataMap.put(variableName, val);

        Map<Long, Integer> variableHistory = new HashMap<>();
        variableHistory.put(Long.valueOf(0), val);

        writeHistory.put(variableName, variableHistory);
    }

    /**
     * Calls the lockTable to release the read lock on the given variable by the given transaction
     * @author Saumya
     */
    boolean releaseReadLock(String variableName, String transactionName) {
        return lockTable.releaseReadLock(variableName, transactionName);
    }

    /**
     * Calls the lockTable to release the write lock on the given variable
     * @author Saumya
     */
    boolean releaseWriteLock(String variableName) {
        return lockTable.releaseWriteLock(variableName);
    }

    /**
     * Calls the lockTable to get the read lock on the given variable by the given transaction
     * @author Saumya
     */
    boolean getReadLock(String variableName, String transactionId) {
        return lockTable.addReadLock(variableName, transactionId);
    }

    /**
     * Calls the lockTable to get the write lock on the given variable by the given transaction
     * @author Saumya
     */
    boolean getWriteLock(String variableName, String transactionId) {
        return lockTable.addWriteLock(variableName, transactionId);
    }

    /**
     * @author Saumya
     */
    void clearAllLocks() {
        lockTable.clearLockTable();
    }

    /**
     * Retrieves and returns the current writeLockHolder information from the lockTable
     * @author Saumya
     */
    Optional<String> getWriteLockHolder(String variableName) {
        return lockTable.getWriteLockHolder(variableName);
    }

    /**
     * Retrieves and returns the current readLockHolder information from the lockTable
     * @author Saumya
     */
    List<String> getReadLockHolders(String variableName) {
        return lockTable.getReadLockHolders(variableName);
    }

    /**
     * Dumps the current state of the data on the site to stdout
     * @author Saumya
     */
    void dumpSite(int id) {
        StringBuffer sb = new StringBuffer();
        sb.append("site " + id + " - ");
        String[] variableList = new String[dataMap.size()];
        dataMap.keySet().toArray(variableList);
        Arrays.sort(variableList, Comparator.comparingInt((String a) -> Integer.parseInt(a.substring(1))));
        for (String variableName : variableList) {
            int val = dataMap.get(variableName);
            sb.append(variableName + ":" + val + ", ");
        }
        System.out.println(sb.toString());
    }

    /**
     * Checks in the lockTable if the writeLock is available for the given variable by the given transaction
     * @author Saumya
     */
    boolean isWriteLockAvailable(String variableName, String transactionId) {
        return lockTable.isWriteLockAvailable(variableName, transactionId);
    }
}

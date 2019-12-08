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

    public Integer read(String variableName) {
        if (!dataMap.containsKey(variableName)) {
            throw new RuntimeException("Site does not contain variable");
        }
        return dataMap.get(variableName);
    }

    public Optional<Integer> readForRO(String variableName, Long tickTime) {
        if (!dataMap.containsKey(variableName)) {
            throw new RuntimeException("Site does not contain variable");
        }
        Map<Long, Integer> variableHistory = writeHistory.get(variableName);
        if (variableHistory.containsKey(tickTime)) {
            return Optional.of(variableHistory.get(tickTime));
        }
        return Optional.empty();
    }

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

    public boolean isVariableSafeForRead(String variableName) {
        if (unsafeVariablesForReading.contains(variableName)) {
            return false;
        }
        return true;
    }

    public void clearStaleSet() {
        unsafeVariablesForReading.clear();
    }

    public void addVariableToStaleSet(String variableName) {
        if (dataMap.containsKey(variableName)) {
            unsafeVariablesForReading.add(variableName);
        } else {
            throw new RuntimeException("this variable is not present at this site");
        }
    }

    public void initializeVar(String variableName, int val) {
        dataMap.put(variableName, val);

        Map<Long, Integer> variableHistory = new HashMap<>();
        variableHistory.put(Long.valueOf(0), val);

        writeHistory.put(variableName, variableHistory);
    }

    public boolean releaseReadLock(String variableName, String transactionName) {
        return lockTable.releaseReadLock(variableName, transactionName);
    }

    public boolean releaseWriteLock(String variableName) {
        return lockTable.releaseWriteLock(variableName);
    }

    public boolean getReadLock(String variableName, String transactionId) {
        return lockTable.addReadLock(variableName, transactionId);
    }

    public boolean getWriteLock(String variableName, String transactionId) {
        return lockTable.addWriteLock(variableName, transactionId);
    }

    public void clearAllLocks() {
        lockTable.clearLockTable();
    }

    Optional<String> getWriteLockHolder(String variableName) {
        return lockTable.getWriteLockHolder(variableName);
    }

    List<String> getReadLockHolders(String variableName) {
        return lockTable.getReadLockHolders(variableName);
    }

    public void dumpSite(int id) {
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

    public boolean isWriteLockAvailable(String variableName, String transactionId) {
        return lockTable.isWriteLockAvailable(variableName, transactionId);
    }
}

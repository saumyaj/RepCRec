package nyu.edu.adb.project;
import java.util.*;

public class Site {

    private final Map<String, Integer> dataMap;
    private final Set<String> unsafeVariablesForReading;
    private final int id;
    private final LockTable lockTable;

    public Site(int id) {
        dataMap = new HashMap<>();
        unsafeVariablesForReading = new HashSet<>();
        this.id = id;
        lockTable = new LockTable();
    }

    public Integer read(String variableName) {
        if (!dataMap.containsKey(variableName)) {
            throw new RuntimeException("Site does not contain variable");
        }
        return dataMap.get(variableName);
    }

    public void write(String variableName, int val) {
        if (!dataMap.containsKey(variableName)) {
            throw new RuntimeException("Site does not contain variable");
        }
        dataMap.put(variableName, val);
        if (unsafeVariablesForReading.contains(variableName)) {
            unsafeVariablesForReading.remove(variableName);
        }
    }

    public boolean isVariableSafeForRead(String variableName) {
        if(unsafeVariablesForReading.contains(variableName)) {
            return false;
        }
        return true;
    }

    public void clearStaleSet() {
        unsafeVariablesForReading.clear();
    }

    public void addVariableToStaleSet(String variableName) {
        if(dataMap.containsKey(variableName)) {
            unsafeVariablesForReading.add(variableName);
        } else {
            throw new RuntimeException("this variable is not present at this site");
        }
    }

    public void initializeVar(String variableName, int val) {
        dataMap.put(variableName, val);
    }

    public void dumpSite() {
        StringBuffer sb = new StringBuffer();
        sb.append("site " + id + "- ");
        String[] variableList = new String[dataMap.size()];
        dataMap.keySet().toArray(variableList);
        Arrays.sort(variableList, Comparator.comparingInt((String a) -> Integer.parseInt(a.substring(1))));
        for(String variableName: variableList) {
            int val = dataMap.get(variableName);
            sb.append(variableName + ":" + val + ", ");
        }
        System.out.println(sb.toString());
    }

    public boolean releaseReadLock(String variableName) {
        return lockTable.releaseReadLock(variableName);
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

}

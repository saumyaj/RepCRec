package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
        dataMap.put(variableName, val);
        if (unsafeVariablesForReading.contains(variableName)) {

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

    public boolean releaseReadLock(String variableName) {
        return lockTable.releaseReadLock(variableName);
    }

    public boolean releaseWriteLock(String variableName) {
        return lockTable.releaseWriteLock(variableName);
    }

    public boolean getReadLock(String variableName) {
        return lockTable.addReadLock(variableName);
    }

    public boolean getWriteLock(String variableName) {
        return lockTable.addWriteLock(variableName);
    }
}

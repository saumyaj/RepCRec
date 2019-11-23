package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Site {

    public enum Availability {
        DOWN, UP;
    }

    ;
    private final Map<String, Integer> dataMap;
    private final Set<String> unsafeVariablesForReading;
    private Availability availability;
    private final int id;
    private final LockTable lockTable;

    public Site(int id) {
        dataMap = new HashMap<>();
        unsafeVariablesForReading = new HashSet<>();
        availability = Availability.UP;
        this.id = id;
        lockTable = new LockTable();
    }

    public Integer read(String variableName) {
        checkAvailability();
        if (!dataMap.containsKey(variableName)) {
            throw new RuntimeException("Site does not contain variable");
        }
        return dataMap.get(variableName);
    }

    public void write(String variableName, int val) {
        checkAvailability();
        dataMap.put(variableName, val);
        if (unsafeVariablesForReading.contains(variableName)) {

        }
    }

    public Availability getAvailability() {
        return availability;
    }

    public void setAvailability(Availability availability) {
        if (availability == Availability.UP && this.availability == Availability.DOWN) {
            //recovery phase
            unsafeVariablesForReading.clear();
            for (String variableName : dataMap.keySet()) {
                unsafeVariablesForReading.add(variableName);
            }
        }
        this.availability = availability;
    }

    private void checkAvailability() {
        if (availability == Availability.DOWN) {
            throw new RuntimeException("Accessing a site which is not available");
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

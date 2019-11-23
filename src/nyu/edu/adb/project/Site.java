package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.Map;

public class Site {
    private final Map<String, Integer> data;
    private final int id;
    private final LockTable lockTable;

    // TODO - Add a datastructure to maintain read-availability of a variable after a failure-recovery

    Site(int id) {
        this.id = id;
        data = new HashMap<>();
        lockTable = new LockTable();
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

package nyu.edu.adb.project;

import java.util.*;

/**
 * The site has its own data manager and transfers all calls related to its data to the data manager
 */
public class Site {

    private final int id;

    private final DataManager dataManager;

    public Site(int id) {
        this.id = id;
        this.dataManager = new DataManager();
    }

    public Integer read(String variableName) {
        return dataManager.read(variableName);
    }

    public Optional<Integer> readForRO(String variableName, Long tickTime) {
        return dataManager.readForRO(variableName, tickTime);
    }

    public void write(String variableName, int val, long tickTime) {
        dataManager.write(variableName, val, tickTime);
    }

    public boolean isVariableSafeForRead(String variableName) {
        return dataManager.isVariableSafeForRead(variableName);
    }

    public void clearStaleSet() {
        dataManager.clearStaleSet();
    }

    public void addVariableToStaleSet(String variableName) {
        dataManager.addVariableToStaleSet(variableName);
    }

    public void initializeVar(String variableName, int val) {
        dataManager.initializeVar(variableName, val);
    }

    public boolean releaseReadLock(String variableName, String transactionName) {
        return dataManager.releaseReadLock(variableName, transactionName);
    }

    public boolean releaseWriteLock(String variableName) {
        return dataManager.releaseWriteLock(variableName);
    }

    public boolean getReadLock(String variableName, String transactionId) {
        return dataManager.getReadLock(variableName, transactionId);
    }

    public boolean getWriteLock(String variableName, String transactionId) {
        return dataManager.getWriteLock(variableName, transactionId);
    }

    public void clearAllLocks() {
        dataManager.clearAllLocks();
    }

    Optional<String> getWriteLockHolder(String variableName) {
        return dataManager.getWriteLockHolder(variableName);

    }

    List<String> getReadLockHolders(String variableName) {
        return dataManager.getReadLockHolders(variableName);
    }

    public void dumpSite() {
        dataManager.dumpSite(id);
    }

    public boolean isWriteLockAvailable(String variableName, String transactionId) {
        return dataManager.isWriteLockAvailable(variableName, transactionId);
    }

}

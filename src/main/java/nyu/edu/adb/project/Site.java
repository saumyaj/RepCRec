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

    Optional<Integer> readForRO(String variableName, Long tickTime) {
        return dataManager.readForRO(variableName, tickTime);
    }

    public void write(String variableName, int val, long tickTime) {
        dataManager.write(variableName, val, tickTime);
    }

    boolean isVariableSafeForRead(String variableName) {
        return dataManager.isVariableSafeForRead(variableName);
    }

    void clearStaleSet() {
        dataManager.clearStaleSet();
    }

    void addVariableToStaleSet(String variableName) {
        dataManager.addVariableToStaleSet(variableName);
    }

    void initializeVar(String variableName, int val) {
        dataManager.initializeVar(variableName, val);
    }

    boolean releaseReadLock(String variableName, String transactionName) {
        return dataManager.releaseReadLock(variableName, transactionName);
    }

    boolean releaseWriteLock(String variableName) {
        return dataManager.releaseWriteLock(variableName);
    }

    boolean getReadLock(String variableName, String transactionId) {
        return dataManager.getReadLock(variableName, transactionId);
    }

    boolean getWriteLock(String variableName, String transactionId) {
        return dataManager.getWriteLock(variableName, transactionId);
    }

    void clearAllLocks() {
        dataManager.clearAllLocks();
    }

    Optional<String> getWriteLockHolder(String variableName) {
        return dataManager.getWriteLockHolder(variableName);

    }

    List<String> getReadLockHolders(String variableName) {
        return dataManager.getReadLockHolders(variableName);
    }

    void dumpSite() {
        dataManager.dumpSite(id);
    }

    boolean isWriteLockAvailable(String variableName, String transactionId) {
        return dataManager.isWriteLockAvailable(variableName, transactionId);
    }

}

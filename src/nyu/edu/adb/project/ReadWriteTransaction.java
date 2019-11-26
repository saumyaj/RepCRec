package nyu.edu.adb.project;

import java.util.*;

class ReadWriteTransaction extends Transaction {
    private HashMap<String, Integer> readLocks;
    private HashMap<String, Integer> writeLocks;
    private Set<Integer> sitesAccessed;
    private Map<String, Integer> modifiedVariables;
    private boolean isAborted;

    ReadWriteTransaction(String name, long tickTime) {
        super(name, tickTime);
        readLocks = new HashMap<>();
        writeLocks = new HashMap<>();
        sitesAccessed = new HashSet<>();
        modifiedVariables = new HashMap<>();
        isAborted = false;
    }

    public void writeToVariable(String variableName, int variableValue) throws Exception {
        if (!writeLocks.containsKey(variableName)) {
            throw new Exception("Transaction " + getName() + " has not acquired the write lock for variable " + variableName);
        }
        modifiedVariables.put(variableName, variableValue);
    }

    public HashMap<String, Integer> getReadLocks() {
        return readLocks;
    }

    int getReadLockSiteId(String variableName) {
        if(!readLocks.containsKey(variableName)) {
            throw new RuntimeException("Read lock not acquired for this transaction");
        }
        return readLocks.get(variableName);
    }

    int getWriteLockSiteId(String variableName) {
        if(!writeLocks.containsKey(variableName)) {
            throw new RuntimeException("Write lock not acquired for this transaction");
        }
        return readLocks.get(variableName);
    }

    public Set<String> getWriteLocks() {
        return writeLocks.keySet();
    }

    public boolean isAborted() {
        return isAborted;
    }

    public void setAborted(boolean aborted) {
        isAborted = aborted;
    }

    public Map<String, Integer> getModifiedVariables() {
        return modifiedVariables;
    }

    public Set<Integer> getSitesAccessed() {
        return sitesAccessed;
    }

    public int addReadLock(String variableName, Integer siteId) {
        return readLocks.put(variableName, siteId);
    }

    public int addWriteLock(String variableName, Integer siteId) {
        return writeLocks.put(variableName, siteId);
    }

    public boolean addAccessedSites(List<Integer> listOfSites) {
        return sitesAccessed.addAll(listOfSites);
    }

    public boolean addAccessedSite(int siteId) {
        return sitesAccessed.add(siteId);
    }

    boolean hasReadLock(String variableName) {
        return readLocks.containsKey(variableName);
    }

    boolean hasWriteLock(String variableName) {
        return writeLocks.containsKey(variableName);
    }
}

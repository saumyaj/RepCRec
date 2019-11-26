package nyu.edu.adb.project;

import java.util.*;

class ReadWriteTransaction extends Transaction {
    private HashMap<String, Integer> readLocks;
    private Set<String> writeLocks;
    private Set<Integer> sitesAccessed;
    private Map<String, Integer> modifiedVariables;
    private boolean isAborted;

    ReadWriteTransaction(String name, long tickTime) {
        super(name, tickTime);
        readLocks = new HashMap<>();
        writeLocks = new HashSet<>();
        sitesAccessed = new HashSet<>();
        modifiedVariables = new HashMap<>();
        isAborted = false;
    }

    public void writeToVariable(String variableName, int variableValue) throws Exception {
        if (!writeLocks.contains(variableName)) {
            throw new Exception("Transaction " + getName() + " has not acquired the write lock for variable " + variableName);
        }
        modifiedVariables.put(variableName, variableValue);
    }

    public HashMap<String, Integer> getReadLocks() {
        return readLocks;
    }

    public int getReadLockSiteId(String variableName) {
        if(!readLocks.containsKey(variableName)) {
            throw new RuntimeException("Read lock not acquired for this transaction");
        }
        return readLocks.get(variableName);
    }

    public Set<String> getWriteLocks() {
        return writeLocks;
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

    public boolean addWriteLock(String variableName) {
        return writeLocks.add(variableName);
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
        return writeLocks.contains(variableName);
    }
}

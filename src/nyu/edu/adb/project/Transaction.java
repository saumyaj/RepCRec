package nyu.edu.adb.project;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;

class Transaction {
    private Set<String> readLocks;
    private Set<String> writeLocks;
    private Set<String> sitesAccessed;

    private Map<String, Variable> modifiedVariables;

    String id;

    long beginTime;

    Transaction(String id, long tickTime) {
        this.id = id;
        readLocks = new HashSet<>();
        writeLocks = new HashSet<>();
        sitesAccessed = new HashSet<>();
        modifiedVariables = new HashMap<>();
        beginTime = tickTime;
    }

    public void writeToVariable(String variableName, int variableValue) throws Exception {
        if (!writeLocks.contains(variableName)) {
            throw new Exception("Transaction " + id + " has not acquired the write lock for variable " + variableName);
        }

        Variable v = new Variable(variableName, variableValue);
        modifiedVariables.put(variableName, v);
    }

    public Set<String> getReadLocks() {
        return readLocks;
    }

    public Set<String> getWriteLocks() {
        return writeLocks;
    }

    public Set<String> getSitesAccessed() {
        return sitesAccessed;
    }

    public boolean addReadLock(String variableName) {
        return readLocks.add(variableName);
    }

    public boolean addWriteLock(String variableName) {
        return writeLocks.add(variableName);
    }

    public boolean addAccessedSite(String siteName) {
        return sitesAccessed.add(siteName);
    }
}
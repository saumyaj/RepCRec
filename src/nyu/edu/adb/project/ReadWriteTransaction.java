package nyu.edu.adb.project;

import java.util.*;

class ReadWriteTransaction extends Transaction {
    private Set<String> readLocks;
    private Set<String> writeLocks;
    private Set<Integer> sitesAccessed;
    private Map<String, Variable> modifiedVariables;

    ReadWriteTransaction(String id, long tickTime) {
        super(id, tickTime);
        readLocks = new HashSet<>();
        writeLocks = new HashSet<>();
        sitesAccessed = new HashSet<>();
        modifiedVariables = new HashMap<>();
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

    public Set<Integer> getSitesAccessed() {
        return sitesAccessed;
    }

    public boolean addReadLock(String variableName) {
        return readLocks.add(variableName);
    }

    public boolean addWriteLock(String variableName) {
        return writeLocks.add(variableName);
    }

    public boolean addAccessedSites(List<Integer> listOfSites) {
        return sitesAccessed.addAll(listOfSites);
    }

    boolean hasReadLock(String variableName) {
        return readLocks.contains(variableName);
    }

    boolean hasWriteLock(String variableName) {
        return writeLocks.contains(variableName);
    }
}

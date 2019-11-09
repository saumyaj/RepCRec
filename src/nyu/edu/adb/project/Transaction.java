package nyu.edu.adb.project;

import java.util.List;

class Transaction {
    private List<String> readLocks;
    private List<String> writeLocks;
    private List<String> sitesAccessed;

    public List<String> getReadLocks() {
        return readLocks;
    }

    public List<String> getWriteLocks() {
        return writeLocks;
    }

    public List<String> getSitesAccessed() {
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
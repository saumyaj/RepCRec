package nyu.edu.adb.project;

import java.util.*;

class ReadWriteTransaction extends Transaction {
    private Map<String, Integer> readLocks;
    private Map<String, List<Integer>> writeLocks;
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


    /**
     * Adds the given value to variable value map which stores all the transaction's writes
     * @param variableName Name of the variable
     * @param variableValue Value of the variable
     * @author Omkar
     */
    void writeToVariable(String variableName, int variableValue) {
        if (!writeLocks.containsKey(variableName)) {
            return;
//            throw new Exception("Transaction " + getName() + " has not acquired the write lock for variable " + variableName);
        }
        modifiedVariables.put(variableName, variableValue);
    }

    Map<String, Integer> getReadLocks() {
        return readLocks;
    }

    /**
     * Gets the site id where read lock for given variable is held
     * @param variableName The name of the variable
     * @return Site id
     * @author Omkar
     */
    int getReadLockSiteId(String variableName) {
        if(!readLocks.containsKey(variableName)) {
            throw new RuntimeException("Read lock not acquired for this transaction");
        }
        return readLocks.get(variableName);
    }

    /**
     * gets the list of site ids where write lock for given variable is held
     * @param variableName The name of the variable
     * @return List of Site ids
     * @author Omkar
     */
    List<Integer> getWriteLockSiteId(String variableName) {
        if(!writeLocks.containsKey(variableName)) {
            throw new RuntimeException("Write lock not acquired for this transaction");
        }
        return writeLocks.get(variableName);
    }

    Map<String, List<Integer>>  getWriteLocks() {
        return writeLocks;
    }

    public boolean isAborted() {
        return isAborted;
    }

    public void setAborted(boolean aborted) {
        isAborted = aborted;
    }

    Map<String, Integer> getModifiedVariables() {
        return modifiedVariables;
    }

    public Set<Integer> getSitesAccessed() {
        return sitesAccessed;
    }


    /**
     * Adds read lock for specific site
     * @param variableName Name of the variable
     * @param siteId Site id
     * @author Omkar
     */
    void addReadLock(String variableName, Integer siteId) {
        readLocks.put(variableName, siteId);
        return;
    }

    /**
     * Adds write lock for given sites
     * @param variableName Name of the variable
     * @param siteIdList Site ids
     * @author Omkar
     */
    void addWriteLock(String variableName, List<Integer> siteIdList) {
        writeLocks.put(variableName, siteIdList);
        return;
    }

    /**
     * Adds given sites to list of accessed sites
     * @param listOfSites
     * @return true if sites were not already present in the list, false otherwise
     * @author Omkar
     */
    boolean addAccessedSites(List<Integer> listOfSites) {
        return sitesAccessed.addAll(listOfSites);
    }

    /**
     * Adds given site to list of accessed sites
     * @param siteId Site Id of Site which was accessed
     * @return true if site was not already present in the list, false otherwise
     * @author Omkar
     */
    boolean addAccessedSite(int siteId) {
        return sitesAccessed.add(siteId);
    }

    /**
     * Checks if transaction has read lock on given variable
     * @param variableName Name of variable
     * @return true if transaction has read lock on given variable, false otherwise
     * @author Omkar
     */
    boolean hasReadLock(String variableName) {
        return readLocks.containsKey(variableName);
    }

    /**
     * Checks if transaction has write lock on given variable
     * @param variableName Name of variable
     * @return true if transaction has write lock on given variable, false otherwise
     * @author Omkar
     */
    boolean hasWriteLock(String variableName) {
        return writeLocks.containsKey(variableName);
    }
}

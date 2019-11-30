package nyu.edu.adb.project;

import java.util.*;


// TODO - This class should handle the decision of which variable copies are safe to read after recovery
class SiteManager {
    enum Status {
        UP, DOWN
    }
    private Map<Integer, Site> siteMap;
    private Map<String, List<Integer>> variableToSiteIdMap;
    private Map<Integer, Status> siteStatusMap;
    private final int NUMBER_OF_SITES;
    private Set<String> replicatedVariables;
    private TransactionManager transactionManager;

    SiteManager(int NUMBER_OF_SITES) {
        this.NUMBER_OF_SITES = NUMBER_OF_SITES;
        siteMap = new HashMap<>();
        variableToSiteIdMap = new HashMap<>();
        siteStatusMap = new HashMap<>();
        for(int i=1;i<=NUMBER_OF_SITES;i++) {
            siteMap.put(i, new Site(i));
        }
        replicatedVariables = new HashSet<>();
    }

    void failSite(int siteId) {
        Site site = siteMap.get(siteId);
        site.clearAllLocks();
        siteStatusMap.put(siteId, Status.DOWN);
    }

    public void recoverSite(int siteId) {
        siteStatusMap.put(siteId, Status.UP);
        Site site = siteMap.get(siteId);
        site.clearAllLocks();
        site.clearStaleSet();
        for(String variableName : variableToSiteIdMap.keySet()) {
            if(replicatedVariables.contains(variableName)) {
                site.addVariableToStaleSet(variableName);
            }
            transactionManager.processWaitingOperationsIfAny(variableName);
        }
    }

    /*
     * This function tries to get read lock on first possible site and returns its id or returns null if no site is
     * available.
     */
    Optional<Integer> getReadLock(String variableName, String transactionId) {
        List<Integer> listOfSiteIds = variableToSiteIdMap.get(variableName);

        for (Integer siteId: listOfSiteIds) {
            Site site = siteMap.get(siteId);
            if (siteStatusMap.get(siteId).equals(Status.UP)
                    && site.isVariableSafeForRead(variableName)
                    && site.getReadLock(variableName, transactionId)) {
                return Optional.of(siteId);
            }

        }
        return Optional.empty();
    }

    /**
     *
     * @param variableName
     * @return List of site ids where the write lock was successfully acquired
     */
    public List<Integer> getWriteLock(String variableName, String transactionId) {
        List<Integer> listOfSiteIds = variableToSiteIdMap.get(variableName);
        List<Integer> listOfSiteIdWhereLockAcquired = new ArrayList<>();
        for (Integer siteId: listOfSiteIds) {
            Site site = siteMap.get(siteId);
            if (siteStatusMap.get(siteId).equals(Status.DOWN)) {
                continue;
            }
            if (site.getWriteLock(variableName, transactionId)) {
                listOfSiteIdWhereLockAcquired.add(siteId);
            }
        }
        return listOfSiteIdWhereLockAcquired;
    }

    public Optional<Integer> read(String variableName, int siteId) {
        Site site = siteMap.get(siteId);
        if(siteStatusMap.get(siteId).equals(Status.DOWN)) {
            return Optional.empty();
        }
        return Optional.of(site.read(variableName));
    }

    public boolean commitWrites( Map<String, Integer> modifiedVariables) {
        for(String variableName: modifiedVariables.keySet()) {
            int variableValue = modifiedVariables.get(variableName);
            List<Integer> sites = variableToSiteIdMap.get(variableName);
            for(int siteId: sites) {
                if(siteStatusMap.get(siteId).equals(Status.UP)){
                    Site site = siteMap.get(siteId);
                    site.write(variableName, variableValue);
                }
            }
        }
        return true;
    }

    public void releaseReadLock(String variableName, int siteId, String transactionName) {
        Site site = siteMap.get(siteId);
        if(siteStatusMap.get(siteId).equals(Status.UP)){
            site.releaseReadLock(variableName, transactionName);
        }
    }

    public void releaseWriteLock(String variableName, int siteId) {
        Site site = siteMap.get(siteId);
        if(siteStatusMap.get(siteId).equals(Status.UP)){
            site.releaseWriteLock(variableName);
        }
    }

    Optional<String> getWriteLockHolder(String variableName) {
        List<Integer> siteIds = variableToSiteIdMap.get(variableName);
        Optional<String> writeLockHolder = Optional.empty();
        for(Integer siteId: siteIds) {
            if (siteStatusMap.get(siteId).equals(Status.UP)) {
                Site site = siteMap.get(siteId);
                if (site.getWriteLockHolder(variableName).isPresent()) {
                    return site.getWriteLockHolder(variableName);
                }
            }
        }
        return writeLockHolder;
    }

    List<String> getReadLockHolders(String variableName) {
        List<Integer> siteIds = variableToSiteIdMap.get(variableName);
        List<String> readLockHolders = new ArrayList<>();
        for(Integer siteId: siteIds) {
            if (siteStatusMap.get(siteId).equals(Status.UP)) {
                Site site = siteMap.get(siteId);
                readLockHolders.addAll(site.getReadLockHolders(variableName));
            }
        }
        return readLockHolders;
    }

    void initializeVariables() {
        final int NUMBER_OF_VARIABLES = 20;
        for(int var = 1; var <= NUMBER_OF_VARIABLES; var++) {
            String variableName = "x" + var;
            List<Integer> listOfSites = new ArrayList<>();
            int variableValue = var*10;
            if(var % 2 == 0) {
                for (int siteId = 1; siteId <= NUMBER_OF_SITES; siteId++) {
                    listOfSites.add(siteId);
                    Site site = siteMap.get(siteId);
                    site.initializeVar(variableName, variableValue);
                }
                replicatedVariables.add(variableName);
            } else {
                int siteId = 1 + var%10;
                listOfSites.add(siteId);
                Site site = siteMap.get(siteId);
                site.initializeVar(variableName, variableValue);
            }
            variableToSiteIdMap.put(variableName, listOfSites);
        }
        for(int i=1;i<=NUMBER_OF_SITES;i++) {
            siteStatusMap.put(i, Status.UP);
        }
    }

    void dump() {
        for(Site site: siteMap.values()) {
            site.dumpSite();
        }
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

}

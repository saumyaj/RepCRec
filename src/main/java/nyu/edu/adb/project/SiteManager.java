package nyu.edu.adb.project;

import java.util.*;

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
    private HashMap<String, Long> lastWriteMap;

    SiteManager(int NUMBER_OF_SITES) {
        this.NUMBER_OF_SITES = NUMBER_OF_SITES;
        siteMap = new HashMap<>();
        variableToSiteIdMap = new HashMap<>();
        siteStatusMap = new HashMap<>();
        for (int i = 1; i <= NUMBER_OF_SITES; i++) {
            siteMap.put(i, new Site(i));
        }
        replicatedVariables = new HashSet<>();
        lastWriteMap = new HashMap<>();
    }

    /**
     * Clears all locks of the given site and sets its status to DOWN
     * @author Saumya
     */
    void failSite(int siteId) {
        Site site = siteMap.get(siteId);
        site.clearAllLocks();
        siteStatusMap.put(siteId, Status.DOWN);
    }

    /**
     * Performs the process of site recovery, updating stale set of variables and checking for any waiting operations
     * @author Omkar
     */
    void recoverSite(int siteId) {
        siteStatusMap.put(siteId, Status.UP);
        Site site = siteMap.get(siteId);
        site.clearAllLocks();
        site.clearStaleSet();
        for (String variableName : variableToSiteIdMap.keySet()) {
            if (replicatedVariables.contains(variableName)) {
                site.addVariableToStaleSet(variableName);
            }
            transactionManager.processWaitingOperationsIfAny(variableName);
        }
        transactionManager.checkROTransactionsForWaitingOperations(siteId);
    }

    /**
     * This function tries to get read lock on first possible site and returns its id or returns null if no site is
     * available.
     * @author Saumya
     */
    Optional<Integer> getReadLock(String variableName, String transactionId) {
        List<Integer> listOfSiteIds = variableToSiteIdMap.get(variableName);

        for (Integer siteId : listOfSiteIds) {
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
     * Checks if all up sites can provide write locks
     * @author Saumya
     */
    boolean canAllUpSitesProvideWriteLock(String variableName, String transactionId) {
        List<Integer> listOfSiteIds = variableToSiteIdMap.get(variableName);
        for (Integer siteId : listOfSiteIds) {
            Site site = siteMap.get(siteId);
            if (siteStatusMap.get(siteId).equals(Status.UP)
                    && !site.isWriteLockAvailable(variableName, transactionId)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks and returns the site ids where the write lock was successfully acquired
     * @author Saumya
     */
    List<Integer> getWriteLock(String variableName, String transactionId) {
        List<Integer> listOfSiteIdWhereLockAcquired = new ArrayList<>();

        // First check if writeLock is available on all the site
        // This keeps site manager from acquiring partial writeLocks
        if (!canAllUpSitesProvideWriteLock(variableName, transactionId)) {
            return listOfSiteIdWhereLockAcquired;
        }

        List<Integer> listOfSiteIds = variableToSiteIdMap.get(variableName);

        for (Integer siteId : listOfSiteIds) {
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

    /**
     * Issues a read for given variable on a particular site
     * @author Omkar
     */
    public Optional<Integer> read(String variableName, int siteId) {
        Site site = siteMap.get(siteId);
        if (siteStatusMap.get(siteId).equals(Status.DOWN)) {
            return Optional.empty();
        }
        return Optional.of(site.read(variableName));
    }

    /**
     * Issues a read for given variable and tickTime for a read only transaction
     * @author Omkar
     */
    Optional<Integer> readForRO(String variableName, Long tickTime) {
        List<Integer> listOfSiteIds = variableToSiteIdMap.get(variableName);
        for (int siteId : listOfSiteIds) {
            Site site = siteMap.get(siteId);
            if (siteStatusMap.get(siteId).equals(Status.UP)) {
                Optional<Integer> val = site.readForRO(variableName, tickTime);
                if (val.isPresent()) {
                    return val;
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Issues a read for given variable on a particular site with particular tickTime for a read only transaction
     * @author Omkar
     */
    Optional<Integer> readForROFromSpecificSite(String variableName, Long tickTime, int siteId) {
        Site site = siteMap.get(siteId);
        if (siteStatusMap.get(siteId).equals(Status.UP)) {
            return site.readForRO(variableName, tickTime);
        }
        return Optional.empty();
    }

    /**
     * Commits writes for given variables and locks acquired
     * @author Omkar
     */
    void commitWrites(Map<String, Integer> modifiedVariables, Map<String, List<Integer>> writeLocks,
                             long tickTime) {
        for (String variableName : modifiedVariables.keySet()) {
            int variableValue = modifiedVariables.get(variableName);
            List<Integer> sites = writeLocks.get(variableName);
            for (int siteId : sites) {
                if (siteStatusMap.get(siteId).equals(Status.UP)) {
                    Site site = siteMap.get(siteId);
                    site.write(variableName, variableValue, tickTime);
                }
            }
            lastWriteMap.put(variableName, tickTime);
        }
    }

    /**
     * Transfers a release read lock call to particular site
     * @author Saumya
     */
    void releaseReadLock(String variableName, int siteId, String transactionName) {
        Site site = siteMap.get(siteId);
        if (siteStatusMap.get(siteId).equals(Status.UP)) {
            site.releaseReadLock(variableName, transactionName);
        }
    }

    /**
     * Transfers a release write lock call to particular site
     * @author Omkar
     */
    void releaseWriteLock(String variableName, int siteId) {
        Site site = siteMap.get(siteId);
        if (siteStatusMap.get(siteId).equals(Status.UP)) {
            site.releaseWriteLock(variableName);
        }
    }

    /**
     * Finds the write lock holder for given variable
     * @author Saumya
     */
    Optional<String> getWriteLockHolder(String variableName) {
        List<Integer> siteIds = variableToSiteIdMap.get(variableName);
        Optional<String> writeLockHolder = Optional.empty();
        for (Integer siteId : siteIds) {
            if (siteStatusMap.get(siteId).equals(Status.UP)) {
                Site site = siteMap.get(siteId);
                if (site.getWriteLockHolder(variableName).isPresent()) {
                    return site.getWriteLockHolder(variableName);
                }
            }
        }
        return writeLockHolder;
    }

    /**
     * Finds the read lock holder for given variable
     * @author Saumya
     */
    List<String> getReadLockHolders(String variableName) {
        List<Integer> siteIds = variableToSiteIdMap.get(variableName);
        List<String> readLockHolders = new ArrayList<>();
        for (Integer siteId : siteIds) {
            if (siteStatusMap.get(siteId).equals(Status.UP)) {
                Site site = siteMap.get(siteId);
                readLockHolders.addAll(site.getReadLockHolders(variableName));
            }
        }
        return readLockHolders;
    }

    /**
     * Updates the data in the sites and variable to site mapping along with some initialization
     * @author Omkar
     */
    void initializeVariables() {
        final int NUMBER_OF_VARIABLES = 20;
        for (int var = 1; var <= NUMBER_OF_VARIABLES; var++) {
            String variableName = "x" + var;
            List<Integer> listOfSites = new ArrayList<>();
            int variableValue = var * 10;
            if (var % 2 == 0) {
                for (int siteId = 1; siteId <= NUMBER_OF_SITES; siteId++) {
                    listOfSites.add(siteId);
                    Site site = siteMap.get(siteId);
                    site.initializeVar(variableName, variableValue);
                }
                replicatedVariables.add(variableName);
            } else {
                int siteId = 1 + var % 10;
                listOfSites.add(siteId);
                Site site = siteMap.get(siteId);
                site.initializeVar(variableName, variableValue);
            }
            variableToSiteIdMap.put(variableName, listOfSites);
            lastWriteMap.put(variableName, Long.valueOf(0));
        }
        for (int i = 1; i <= NUMBER_OF_SITES; i++) {
            siteStatusMap.put(i, Status.UP);
        }
    }

    void dump() {
        for (Site site : siteMap.values()) {
            site.dumpSite();
        }
    }

    void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    Map<String, Long> getLastWriteMapClone() {
        return (Map<String, Long>) lastWriteMap.clone();
    }
}

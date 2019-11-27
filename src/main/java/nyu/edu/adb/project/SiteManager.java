package nyu.edu.adb.project;

import javax.swing.text.html.Option;
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

    SiteManager(int NUMBER_OF_SITES) {
        this.NUMBER_OF_SITES = NUMBER_OF_SITES;
        siteMap = new HashMap<>();
        variableToSiteIdMap = new HashMap<>();
        siteStatusMap = new HashMap<>();
        for(int i=1;i<=NUMBER_OF_SITES;i++) {
            siteMap.put(i, new Site(i));
        }
    }

    void failSite(int siteId) {
        siteStatusMap.put(siteId, Status.DOWN);
    }

    public void recoverSite(int siteId) {
        siteStatusMap.put(siteId, Status.UP);
        Site site = siteMap.get(siteId);
        site.clearStaleSet();
        for(String variableName : variableToSiteIdMap.keySet()) {
            List<Integer> sites = variableToSiteIdMap.get(variableName);
            int availableCopies = 0;
            for(Integer variableSite: sites) {
                if(siteStatusMap.get(variableSite).equals(Status.UP)) {
                    availableCopies++;
                }
            }
            if(availableCopies > 1) {
                site.addVariableToStaleSet(variableName);
            }
        }
        site.clearAllLocks();
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

    public void releaseReadLock(String variableName, int siteId) {
        Site site = siteMap.get(siteId);
        if(siteStatusMap.get(siteId).equals(Status.UP)){
            site.releaseReadLock(variableName);
        }
    }

    public void releaseWriteLock(String variableName, int siteId) {
        Site site = siteMap.get(siteId);
        if(siteStatusMap.get(siteId).equals(Status.UP)){
            site.releaseWriteLock(variableName);
        }
    }
}

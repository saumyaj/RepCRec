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

    void recoverSite(int siteId) {
        siteStatusMap.put(siteId, Status.UP);
    }

    /**
     *
     * @param variableName
     * @return The id of the site that the transaction will be accessing; -1 if no site available for reading
     *
     */
    public int getReadLock(String variableName) {
        List<Integer> listOfSiteIds = variableToSiteIdMap.get(variableName);
        for (Integer siteId: listOfSiteIds) {
            Site site = siteMap.get(siteId);
            if (siteStatusMap.get(siteId).equals(Status.UP) && site.getReadLock(variableName)) {
                return siteId;
            }
        }
        return -1;
    }

    /**
     *
     * @param variableName
     * @return List of site ids where the write lock was successfully acquired
     */
    public List<Integer> getWriteLock(String variableName) {
        List<Integer> listOfSiteIds = variableToSiteIdMap.get(variableName);
        List<Integer> listOfSiteIdWhereLockAcquired = new ArrayList<>();
        for (Integer siteId: listOfSiteIds) {
            Site site = siteMap.get(siteId);
            if (siteStatusMap.get(siteId).equals(Status.DOWN)) {
                continue;
            }
            if (site.getWriteLock(variableName)) {
                listOfSiteIdWhereLockAcquired.clear();
                return listOfSiteIdWhereLockAcquired;
            }
            listOfSiteIdWhereLockAcquired.add(siteId);
        }
        return listOfSiteIdWhereLockAcquired;
    }

    Optional<Integer> read(String variableName) {
        //TODO - Find a suitable copy to read and return the appropriate value;
        return Optional.empty();
    }
}

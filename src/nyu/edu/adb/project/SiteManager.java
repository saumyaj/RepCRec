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
    }

    /**
     *
     * @param variableName
     * @return The id of the site that the transaction will be accessing; -1 if no site available for reading
     *
     */
    public int getReadLock(String variableName) {
        List<Integer> listOfSiteIds = variableToSiteIdMap.get(variableName);
        int availableCopies = 0;
        for(Integer siteId: listOfSiteIds) {
            if(siteStatusMap.get(siteId).equals(Status.DOWN)) {
                continue;
            }
            availableCopies++;
        }

        for (Integer siteId: listOfSiteIds) {
            Site site = siteMap.get(siteId);
            if (siteStatusMap.get(siteId).equals(Status.DOWN)) {
                continue;
            }
            if (availableCopies==1 && site.getReadLock(variableName)) {
                return siteId;
            } else if(availableCopies==1 && !site.getReadLock(variableName)) {
                continue;
            } else if(availableCopies>1 && site.isVariableSafeForRead(variableName)) {
                if(site.getReadLock(variableName)) {
                    return siteId;
                }
                continue;
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

    public Optional<Integer> read(String variableName, int siteId) {
        Site site = siteMap.get(siteId);
        if(siteStatusMap.get(siteId).equals(Status.DOWN)) {
            return Optional.empty();
        }
        return Optional.of(site.read(variableName));
    }
}

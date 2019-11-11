package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.Map;

public class LockManager {
    Map<String, LockInfo> variableLockInfoMap;

    LockManager() {
        variableLockInfoMap = new HashMap<>();
    }

    public boolean getWriteLock(String transactionName, String variableName) throws Exception {
        LockInfo lockInfo = variableLockInfoMap.get(variableName);
        if (!lockInfo.getWriteLockHolder().isPresent()) {
            lockInfo.setWriteLockHolder(transactionName);
            return true;
        }
        // TODO - add the transaction/operation info to the queue in the lockInfo object if lock held by other transaction
        return false;
    }

    public void releaseWriteLock(String transactionName, String variableName) throws Exception {
        LockInfo lockInfo = variableLockInfoMap.get(variableName);
        lockInfo.releaseWriteLock(transactionName);
    }

    public boolean getReadLock(String transactionName, String variableName) {
        LockInfo lockInfo = variableLockInfoMap.get(variableName);
        // TODO - finish the method
        return false;
    }
}

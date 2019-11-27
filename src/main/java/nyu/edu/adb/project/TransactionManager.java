package nyu.edu.adb.project;

import javax.swing.text.html.Option;
import java.util.*;

public class TransactionManager {
    Map<String, Transaction> transactionMap;
    SiteManager siteManager;
    DataManager dataManager;
    DeadLockManager deadLockManager;

    TransactionManager(SiteManager siteManager, DataManager dataManager) {
        transactionMap = new HashMap<>();
        this.siteManager = siteManager;
        this.dataManager = dataManager;
        this.deadLockManager = new DeadLockManager();
    }

    void runDeadLockDetection() {
        List<List<String>> cycles = deadLockManager.getDeadLockCycles();
        for (List<String> cycle: cycles) {
            String transactionToBeAborted = findYoungestTransaction(cycle);
            abortTransaction(transactionToBeAborted);
        }
    }

    private void abortTransaction(String transactionName) {
        deadLockManager.removeNode(transactionName);
        dataManager.removeAllPendingOperationOfTransaction(transactionName);

        Transaction transaction = transactionMap.get(transactionName);
        if (transaction instanceof ReadOnlyTransaction) {
            return;
        }

        releaseAllReadLocksOfTransaction((ReadWriteTransaction)transaction);
        releaseAllWriteLocksOfTransaction((ReadWriteTransaction)transaction);
    }

    private void releaseAllReadLocksOfTransaction(ReadWriteTransaction transaction) {
        Map<String, Integer> readLocks = transaction.getReadLocks();
        for (Map.Entry<String, Integer> lock: readLocks.entrySet()) {
            siteManager.releaseReadLock(lock.getKey(), lock.getValue());
        }
    }

    private void releaseAllWriteLocksOfTransaction(ReadWriteTransaction transaction) {
        Map<String, List<Integer>> readLocks = transaction.getWriteLocks();
        for (Map.Entry<String, List<Integer>> lock: readLocks.entrySet()) {
            String variableName = lock.getKey();
            for (Integer siteId: lock.getValue()) {
                siteManager.releaseWriteLock(variableName, siteId);
            }
        }
    }

    private String findYoungestTransaction(List<String> transactionNames) {
        String youngestTransaction = null;
        long yougestAge = Long.MIN_VALUE;
        for (String transactionName: transactionNames) {
            Transaction transaction = transactionMap.get(transactionName);
            if (transaction.getBeginTime() > yougestAge) {
                yougestAge = transaction.getBeginTime();
                youngestTransaction = transactionName;
            }
        }
        return youngestTransaction;
    }

    void createReadWriteTransaction(String transactionName, long tickTime) throws Exception {
        validateTransactionName(transactionName);
        Transaction transaction = new ReadWriteTransaction(transactionName, tickTime);
        transactionMap.put(transactionName, transaction);
    }

    void createReadOnlyTransaction(String transactionName, long tickTime) throws Exception {
        validateTransactionName(transactionName);
        Transaction transaction = new ReadOnlyTransaction(transactionName, tickTime);
        transactionMap.put(transactionName, transaction);
    }

    private void validateTransactionName(String transactionName) throws IllegalArgumentException {
        if (transactionMap.containsKey(transactionName)) {
            throw new IllegalArgumentException("Transaction with name " + transactionName + " already exists");
        }
    }

    void write(String transactionName, String variableName, int value) throws Exception {

        ReadWriteTransaction t;
        if (!(transactionMap.get(transactionName) instanceof ReadWriteTransaction)) {
            throw new IllegalArgumentException("Transaction " + transactionName + " is a ReadOnly Transaction, " +
                    "cannot write");
        }
        t = (ReadWriteTransaction)transactionMap.get(transactionName);

        // TODO - Should we worry about if one of these sites has gone down since the last write?
        // We should probably try to get on all UP site again. In case a new site has come up

//        if (t.getWriteLocks().contains(variableName)) {
//            t.writeToVariable(variableName, value);
//            return;
//        }

        // Check if other transaction is already waiting
        if (dataManager.isOperationAlreadyWaiting(variableName)) {
            handleWaitingForOperation(variableName, transactionName, value);
            return;
        }

        List<Integer> list = siteManager.getWriteLock(variableName, transactionName);
        if (!list.isEmpty()) {
            t.addWriteLock(variableName, list);
            t.addAccessedSites(list);
            t.writeToVariable(variableName, value);
            return;
        }

        // Adding operation to wait queue in order to finish it in future
        handleWaitingForOperation(variableName, transactionName, value);

    }

    private void handleWaitingForOperation(String variableName, String transactionName) {
        Optional<String> lastWriteTransactionFromWaitQueue = dataManager.getLastWriteTransaction(variableName);

        if (lastWriteTransactionFromWaitQueue.isPresent()) {
            deadLockManager.addEdge(transactionName, lastWriteTransactionFromWaitQueue.get());
        } else {
            String writeLockHolder = siteManager.getWriteLockHolder(variableName).get();
            deadLockManager.addEdge(transactionName, writeLockHolder);
        }

        dataManager.addWaitingOperation(variableName,
                new Operation(transactionName, Operation.OperationType.READ, variableName));
    }

    private void handleWaitingForOperation(String variableName, String transactionName, int value) {
        Optional<String> writeLockHolder = siteManager.getWriteLockHolder(variableName);
        List<String> queueHolders = dataManager.getQueueHoldersForWriteOperation(variableName);

        // Adding waits - for edge for the responsible transaction
        if (!queueHolders.isEmpty()) {
            deadLockManager.addMultipleEdges(transactionName, queueHolders);
        } else if (writeLockHolder.isPresent()) {
            deadLockManager.addEdge(transactionName, writeLockHolder.get());
        } else {
            List<String> readLockHolders = siteManager.getReadLockHolders(variableName);
            deadLockManager.addMultipleEdges(transactionName, readLockHolders);
        }
        dataManager.addWaitingOperation(variableName,
                new Operation(transactionName, Operation.OperationType.WRITE, variableName, value));
    }

    Optional<Integer> read(String transactionName, String variableName) throws Exception {
        if (transactionMap.get(transactionName) instanceof  ReadOnlyTransaction ) {
            return Optional.of(
                    readFromReadOnlyTransaction(transactionMap.get(transactionName), variableName)
            );
        }


        ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction)transactionMap.get(transactionName);
        return readFromReadWriteTransaction(readWriteTransaction, variableName);
    }

    /*
     * This method returns the integer value of the required variable or returns null if the variable is unavailable
     */
    private Optional<Integer> readFromReadWriteTransaction(ReadWriteTransaction readWriteTransaction,
                                                           String variableName) {
        Optional<Integer> data;
        Map<String, Integer> previousWrites = readWriteTransaction.getModifiedVariables();
        if(previousWrites.containsKey(variableName)) {
            return Optional.of(previousWrites.get(variableName));
        }

        if (readWriteTransaction.hasReadLock(variableName)) {
            Integer siteId = readWriteTransaction.getReadLockSiteId(variableName);
            data = siteManager.read(variableName, siteId);
            if (data.isPresent()) {
                return data;
            }
        }

        /*
            If we are not given a situation where all the sites fail, this should work. At least one of the sites should
            be up.
        */
        if (readWriteTransaction.hasWriteLock(variableName)) {
            if (readWriteTransaction.getModifiedVariables().containsKey(variableName)) {
                data = Optional.of(readWriteTransaction.getModifiedVariables().get(variableName));
                return data;
            } else {
                // There will never be a case for else. If we get a write lock we must have modified that variable
            }
        }

        String transactionName = readWriteTransaction.getName();

        if (dataManager.isOperationAlreadyWaiting(variableName)) {
            handleWaitingForOperation(variableName, transactionName);
            return Optional.empty();
        }

        final Optional<Integer> siteId = siteManager.getReadLock(variableName, transactionName);

        if(siteId.isPresent()) {
            readWriteTransaction.addReadLock(variableName, siteId.get());
            readWriteTransaction.addAccessedSite(siteId.get());
            return siteManager.read(variableName, siteId.get());
        }


        // Adding operation to wait queue in order to finish it in future
        handleWaitingForOperation(variableName, transactionName);
        return Optional.empty();
    }

    private int readFromReadOnlyTransaction(Transaction transaction, String variableName) {
        ReadOnlyTransaction readOnlyTransaction = (ReadOnlyTransaction)transaction;
        return readOnlyTransaction.getVariable(variableName);
    }

    public void endTransaction(String transactionName) {
        boolean wasCommitted = commitTransaction(transactionName);
        transactionMap.remove(transactionName);
    }

    private boolean commitTransaction(String transactionName) {
        Transaction transaction = transactionMap.get(transactionName);

        if(transaction instanceof ReadWriteTransaction) {
            ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transaction;
            Set<String> writeLockVariablesSet = readWriteTransaction.getWriteLocks().keySet();
            for(String writeLockVariable: writeLockVariablesSet) {
                List<Integer> siteIdList = readWriteTransaction.getWriteLockSiteId(writeLockVariable);
                for(Integer siteId: siteIdList) {
                    siteManager.releaseWriteLock(writeLockVariable, siteId);
                }
            }
        }

        if(transaction instanceof ReadWriteTransaction) {
            ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transaction;
            Set<String> readLockVariablesSet = readWriteTransaction.getReadLocks().keySet();
            for(String readLockVariable: readLockVariablesSet) {
                int siteId = readWriteTransaction.getReadLockSiteId(readLockVariable);
                siteManager.releaseReadLock(readLockVariable, siteId);
            }
        }

        //TODO - remove transaction from waiting queues and graph
        deadLockManager.removeNode(transactionName);
        dataManager.removeAllPendingOperationOfTransaction(transactionName);

        if(transaction instanceof ReadWriteTransaction) {
            ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transaction;
            if(readWriteTransaction.isAborted()) {
                return false;
            }
            return siteManager.commitWrites(readWriteTransaction.getModifiedVariables());
        }
        return true;
    }

    public void checkTransactionsForAbortion(int siteId) {
        for(Transaction transaction: transactionMap.values()) {
            if(transaction instanceof ReadOnlyTransaction) {
                return;
            }
            ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transaction;
            Set<Integer> sitesAccessed = readWriteTransaction.getSitesAccessed();
            if(sitesAccessed.contains(siteId)) {
                readWriteTransaction.setAborted(true);
            }
        }
    }
}

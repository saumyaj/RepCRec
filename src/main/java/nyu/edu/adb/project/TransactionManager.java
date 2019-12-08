package nyu.edu.adb.project;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;

public class TransactionManager {
    Map<String, Transaction> transactionMap;
    SiteManager siteManager;
    WaitQueueManager waitQueueManager;
    DeadLockManager deadLockManager;
    Set<String> abortedTransactions;

    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    TransactionManager(SiteManager siteManager, WaitQueueManager waitQueueManager) {
        transactionMap = new HashMap<>();
        abortedTransactions = new HashSet<>();
        this.siteManager = siteManager;
        this.waitQueueManager = waitQueueManager;
        this.deadLockManager = new DeadLockManager();
    }

    void runDeadLockDetection() {
        List<List<String>> cycles = deadLockManager.getDeadLockCycles();
        while (cycles.size() > 0) {
            LOGGER.log(Level.INFO, "cycle found");
            String transactionToBeAborted = findYoungestTransaction(cycles);
            abortTransactionAfterDeadlock(transactionToBeAborted);
            cycles = deadLockManager.getDeadLockCycles();
        }
    }

    private void abortTransactionAfterDeadlock(String transactionName) {
        LOGGER.log(Level.INFO, "aborting transaction " + transactionName);
        deadLockManager.removeNode(transactionName);
        waitQueueManager.removeAllPendingOperationOfTransaction(transactionName);

        Transaction transaction = transactionMap.get(transactionName);
        if (transaction instanceof ReadOnlyTransaction) {
            return;
        }

        ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transaction;
        releaseResourcesOfReadWriteTransaction(readWriteTransaction);

        abortedTransactions.add(transactionName);
        transactionMap.remove(transactionName);
//        readWriteTransaction.setAborted(true);
//        readWriteTransaction.setAbortReason("Deadlock removal");
        System.out.println(transactionName + " " + "aborts");
        System.out.println("Reason for abortion: " + "Deadlock removal");
    }

    private void releaseAllReadLocksOfTransaction(ReadWriteTransaction transaction) {
        Map<String, Integer> readLocks = transaction.getReadLocks();
        for (Map.Entry<String, Integer> lock : readLocks.entrySet()) {
            siteManager.releaseReadLock(lock.getKey(), lock.getValue(), transaction.getName());
        }
    }

    private void releaseAllWriteLocksOfTransaction(ReadWriteTransaction transaction) {
        Map<String, List<Integer>> readLocks = transaction.getWriteLocks();
        for (Map.Entry<String, List<Integer>> lock : readLocks.entrySet()) {
            String variableName = lock.getKey();
            for (Integer siteId : lock.getValue()) {
                siteManager.releaseWriteLock(variableName, siteId);
            }
        }
    }

    private String findYoungestTransaction(List<List<String>> cycles) {
        String youngestTransaction = null;
        long yougestAge = Long.MIN_VALUE;
        for (List<String> cycle: cycles) {
            for (String transactionName : cycle) {
                Transaction transaction = transactionMap.get(transactionName);
                if (transaction.getBeginTime() > yougestAge) {
                    yougestAge = transaction.getBeginTime();
                    youngestTransaction = transactionName;
                }
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

        Transaction transaction = new ReadOnlyTransaction(transactionName, tickTime, siteManager.getLastWriteMapClone());
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
        t = (ReadWriteTransaction) transactionMap.get(transactionName);


        // We should probably try to get on all UP site again. In case a new site has come up

//        if (t.getWriteLocks().contains(variableName)) {
//            t.writeToVariable(variableName, value);
//            return;
//        }

        // Check if other transaction is already waiting
        if (waitQueueManager.precedingWriteOperationExists(variableName) ||
                !siteManager.canAllUpSitesProvideWriteLock(variableName, transactionName)) {
            handleWaitingForOperation(variableName, transactionName, value);
            return;
        }

        List<Integer> list = siteManager.getWriteLock(variableName, transactionName);
        if (!list.isEmpty()) {

            // TODO - Move these instructions to a function
            t.addWriteLock(variableName, list);
            t.addAccessedSites(list);
            t.writeToVariable(variableName, value);
            return;
        }

        // Adding operation to wait queue in order to finish it in future
        handleWaitingForOperation(variableName, transactionName, value);

    }

    private void handleWaitingForOperation(String variableName, String transactionName) {
        Optional<String> lastWriteTransactionFromWaitQueue = waitQueueManager.getLastWriteTransaction(variableName);

        if (lastWriteTransactionFromWaitQueue.isPresent()) {
            deadLockManager.addEdge(transactionName, lastWriteTransactionFromWaitQueue.get());
        } else {
            Optional<String> writeLockHolder = siteManager.getWriteLockHolder(variableName);
            if (writeLockHolder.isPresent()) {
                deadLockManager.addEdge(transactionName, writeLockHolder.get());
            }
        }

        waitQueueManager.addWaitingOperation(variableName,
                new Operation(transactionName, Operation.OperationType.READ, variableName));
    }

    private void handleWaitingForOperation(String variableName, String transactionName, int value) {
        LOGGER.log(Level.INFO, "Adding write operation for " + transactionName + " to the wait queue");
        Optional<String> writeLockHolder = siteManager.getWriteLockHolder(variableName);
        List<String> queueHolders = waitQueueManager.getQueueHoldersForWriteOperation(variableName);

        // Adding waits - for edge for the responsible transaction
        if (!queueHolders.isEmpty()) {
            deadLockManager.addMultipleEdges(transactionName, queueHolders);
        } else if (writeLockHolder.isPresent()) {
            deadLockManager.addEdge(transactionName, writeLockHolder.get());
        } else {
            List<String> readLockHolders = siteManager.getReadLockHolders(variableName);
            deadLockManager.addMultipleEdges(transactionName, readLockHolders);
        }
        waitQueueManager.addWaitingOperation(variableName,
                new Operation(transactionName, Operation.OperationType.WRITE, variableName, value));
    }

    Optional<Integer> read(String transactionName, String variableName) {
        if (transactionMap.get(transactionName) instanceof ReadOnlyTransaction) {
            return readFromReadOnlyTransaction(transactionMap.get(transactionName), variableName);
        }

        ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transactionMap.get(transactionName);
        return readFromReadWriteTransaction(readWriteTransaction, variableName);
    }

    /*
     * This method returns the integer value of the required variable or returns null if the variable is unavailable
     */
    private Optional<Integer> readFromReadWriteTransaction(ReadWriteTransaction readWriteTransaction,
                                                           String variableName) {
        Optional<Integer> data;
        Map<String, Integer> previousWrites = readWriteTransaction.getModifiedVariables();
        if (previousWrites.containsKey(variableName)) {
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

        if (waitQueueManager.isOperationAlreadyWaiting(variableName)) {
            handleWaitingForOperation(variableName, transactionName);
            return Optional.empty();
        }

        final Optional<Integer> siteId = siteManager.getReadLock(variableName, transactionName);

        if (siteId.isPresent()) {
            readWriteTransaction.addReadLock(variableName, siteId.get());
            readWriteTransaction.addAccessedSite(siteId.get());
            return siteManager.read(variableName, siteId.get());
        }


        // Adding operation to wait queue in order to finish it in future
        handleWaitingForOperation(variableName, transactionName);
        return Optional.empty();
    }

    private Optional<Integer> readFromReadOnlyTransaction(Transaction transaction, String variableName) {
        ReadOnlyTransaction readOnlyTransaction = (ReadOnlyTransaction) transaction;
        Long tickTime = readOnlyTransaction.getVariableTickTIme(variableName);
        Optional<Integer> val = siteManager.readForRO(variableName, tickTime);
        if (!val.isPresent()) {
            readOnlyTransaction.setPendingReadVariable(variableName);
        }
        return val;
    }

    public void checkROTransactionsForWaitingOperations(int siteId) {
        for (String transactionName : transactionMap.keySet()) {
            Transaction transaction = transactionMap.get(transactionName);
            if (transaction instanceof ReadOnlyTransaction) {
                ReadOnlyTransaction readOnlyTransaction = (ReadOnlyTransaction) transaction;
                if (readOnlyTransaction.getPendingReadVariable() != null) {
                    String variableName = readOnlyTransaction.getPendingReadVariable();
                    Long tickTime = readOnlyTransaction.getVariableTickTIme(variableName);
                    Optional<Integer> readValue = siteManager.readForROFromSpecificSite(variableName, tickTime, siteId);
                    if (readValue.isPresent()) {
                        System.out.println(variableName + ": " + readValue.get());
//                        System.out.println(readValue.get());
                    }
                }
            }
        }
    }

    public void endTransaction(String transactionName, long tickTime) {
        if (!transactionMap.containsKey(transactionName)) {
            LOGGER.log(Level.INFO, "Transaction " + transactionName + " not found in Transaction Map");
            return;
        }
        boolean wasCommitted = commitTransaction(transactionName, tickTime);
        if (wasCommitted) {
            LOGGER.log(Level.INFO, "Transaction " + transactionName + " committed successfully");
            System.out.println(transactionName + " commits");
        } else {
            //read only transactions never abort, so this must be a read-write transaction
            ReadWriteTransaction transaction = (ReadWriteTransaction) transactionMap.get(transactionName);
            abortedTransactions.add(transactionName);
            LOGGER.log(Level.INFO, "Transaction " + transactionName + " was aborted");
            System.out.println(transactionName + " aborts");
            System.out.println("Reason for abortion: Site failure");
        }
        transactionMap.remove(transactionName);
    }

    public void processWaitingOperationsIfAny(String variableName) {
        Optional<Operation> nextOp = waitQueueManager.peekAtNextWaitingOperation(variableName);
        if (!nextOp.isPresent()) {
            return;
        }
        Operation operation = nextOp.get();
        if (operation.getOperationType().equals(Operation.OperationType.WRITE)) {
            LOGGER.log(Level.INFO, "Transaction " + operation.getTransactionId() +
                    " trying to get a write lock for " + variableName);
            List<Integer> siteIds = siteManager.getWriteLock(operation.getVariableName(),
                    operation.getTransactionId());

            LOGGER.log(Level.INFO, "Transaction " + operation.getTransactionId() +
                    " got write locks for " + variableName + " on sites " + siteIds.toString());
            // Keep waiting if no site is available
            if (siteIds.isEmpty()) {
                return;
            }

            waitQueueManager.pollNextWaitingOperation(variableName);
            ReadWriteTransaction readWriteTransaction =
                    (ReadWriteTransaction) transactionMap.get(operation.getTransactionId());

            // TODO - Move these instructions to a method
            readWriteTransaction.addWriteLock(variableName, siteIds);
            readWriteTransaction.addAccessedSites(siteIds);
            readWriteTransaction.writeToVariable(variableName, operation.getValue());
        } else {
            Optional<Integer> siteId = siteManager.getReadLock(variableName, operation.getTransactionId());

            if (!siteId.isPresent()) {
                return;
            }

            List<Operation> readOperations = waitQueueManager.pollUntilNextWriteOperation(variableName);
            ReadWriteTransaction readWriteTransaction;
            for (Operation op : readOperations) {
                readWriteTransaction = (ReadWriteTransaction) transactionMap.get(op.getTransactionId());
                readWriteTransaction.addReadLock(variableName, siteId.get());
                readWriteTransaction.addAccessedSite(siteId.get());
                Optional<Integer> value = siteManager.read(variableName, siteId.get());
                System.out.println(variableName + ": " + value.get());
//                System.out.println(value.get());
            }

        }
    }

    private void releaseResourcesOfReadWriteTransaction(ReadWriteTransaction readWriteTransaction) {

        Set<String> writeLockVariablesSet = readWriteTransaction.getWriteLocks().keySet();
        for (String writeLockVariable : writeLockVariablesSet) {
            List<Integer> siteIdList = readWriteTransaction.getWriteLockSiteId(writeLockVariable);
            for (Integer siteId : siteIdList) {
                siteManager.releaseWriteLock(writeLockVariable, siteId);
            }
            processWaitingOperationsIfAny(writeLockVariable);
        }

        Set<String> readLockVariablesSet = readWriteTransaction.getReadLocks().keySet();
        for (String readLockVariable : readLockVariablesSet) {
            int siteId = readWriteTransaction.getReadLockSiteId(readLockVariable);
            siteManager.releaseReadLock(readLockVariable, siteId, readWriteTransaction.getName());
            processWaitingOperationsIfAny(readLockVariable);
        }
    }

    private boolean commitTransaction(String transactionName, long tickTime) {
        Transaction transaction = transactionMap.get(transactionName);
        deadLockManager.removeNode(transactionName);
        waitQueueManager.removeAllPendingOperationOfTransaction(transactionName);

        if (transaction instanceof ReadWriteTransaction) {
            ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transaction;
            if (!readWriteTransaction.isAborted()) {
                siteManager.commitWrites(readWriteTransaction.getModifiedVariables(),
                        readWriteTransaction.getWriteLocks(), tickTime);
            }

            releaseResourcesOfReadWriteTransaction(readWriteTransaction);

            if (readWriteTransaction.isAborted()) {
                return false;
            }
        }
        return true;
    }

    public void checkTransactionsForAbortionAfterSiteFailure(int siteId) {
        for (Transaction transaction : transactionMap.values()) {
            if (transaction instanceof ReadOnlyTransaction) {
                return;
            }
            ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transaction;
            Set<Integer> sitesAccessed = readWriteTransaction.getSitesAccessed();
            if (sitesAccessed.contains(siteId)) {
                readWriteTransaction.setAborted(true);
            }
        }
    }
}

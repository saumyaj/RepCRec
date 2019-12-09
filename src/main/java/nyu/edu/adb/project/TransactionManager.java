package nyu.edu.adb.project;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.*;

class TransactionManager {
    private Map<String, Transaction> transactionMap;
    private SiteManager siteManager;
    private WaitQueueManager waitQueueManager;
    private DeadLockManager deadLockManager;
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

    /**
     * Performs deadlock detection and checks for cycles in waits-for graph
     * @author Saumya
     */
    void runDeadLockDetection() {
        List<List<String>> cycles = deadLockManager.getDeadLockCycles();
        while (cycles.size() > 0) {
            LOGGER.log(Level.INFO, "cycle found");
            String transactionToBeAborted = findYoungestTransaction(cycles);
            abortTransactionAfterDeadlock(transactionToBeAborted);
            cycles = deadLockManager.getDeadLockCycles();
        }
    }

    /**
     * Aborts transaction in order to resolve deadlock
     * @author Saumya
     */
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
        System.out.println(transactionName + " aborts");
        System.out.println("Reason for abortion: Deadlock removal");
    }

    /**
     * Finds youngest transaction in the list of cycles
     * @author Saumya
     */
    private String findYoungestTransaction(List<List<String>> cycles) {
        String youngestTransaction = null;
        long yougestAge = Long.MIN_VALUE;
        for (List<String> cycle : cycles) {
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

    /**
     * Creates a new Read Write Transaction
     * @author Saumya
     */
    void createReadWriteTransaction(String transactionName, long tickTime) {
        validateTransactionName(transactionName);
        Transaction transaction = new ReadWriteTransaction(transactionName, tickTime);
        transactionMap.put(transactionName, transaction);
    }

    /**
     * Creates a new Read-Only Transaction
     * @author Omkar
     */
    void createReadOnlyTransaction(String transactionName, long tickTime) {
        validateTransactionName(transactionName);

        Transaction transaction = new ReadOnlyTransaction(transactionName, tickTime, siteManager.getLastWriteMapClone());
        transactionMap.put(transactionName, transaction);
    }

    /**
     * Validates the name of the transaction
     * @author Saumya
     */
    private void validateTransactionName(String transactionName) throws IllegalArgumentException {
        if (transactionMap.containsKey(transactionName)) {
            throw new IllegalArgumentException("Transaction with name " + transactionName + " already exists");
        }
    }

    /**
     * Performs a write for given transaction if possible or adds to wait queue according to two-phase locking
     * @author Saumya
     */
    void write(String transactionName, String variableName, int value) {
        ReadWriteTransaction t;
        if (!(transactionMap.get(transactionName) instanceof ReadWriteTransaction)) {
            throw new IllegalArgumentException("Transaction " + transactionName + " is a ReadOnly Transaction, " +
                    "cannot write");
        }
        t = (ReadWriteTransaction) transactionMap.get(transactionName);

        if (waitQueueManager.precedingWriteOperationExists(variableName) ||
                !siteManager.canAllUpSitesProvideWriteLock(variableName, transactionName)) {
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

        handleWaitingForOperation(variableName, transactionName, value);
    }

    /**
     * Updates waits-for graph and adds read operation to queue
     * @author Saumya
     */
    private void handleWaitingForOperation(String variableName, String transactionName) {
        Optional<String> lastWriteTransactionFromWaitQueue = waitQueueManager.getLastWriteTransaction(variableName);

        if (lastWriteTransactionFromWaitQueue.isPresent()) {
            deadLockManager.addEdge(transactionName, lastWriteTransactionFromWaitQueue.get());
        } else {
            Optional<String> writeLockHolder = siteManager.getWriteLockHolder(variableName);
            writeLockHolder.ifPresent(s -> deadLockManager.addEdge(transactionName, s));
        }

        waitQueueManager.addWaitingOperation(variableName,
                new Operation(transactionName, Operation.OperationType.READ, variableName));
    }

    /**
     * Updates waits-for graph and adds write operation to queue
     * @author Saumya
     */
    private void handleWaitingForOperation(String variableName, String transactionName, int value) {
        LOGGER.log(Level.INFO, "Adding write operation for " + transactionName + " to the wait queue");
        Optional<String> writeLockHolder = siteManager.getWriteLockHolder(variableName);
        List<String> queueHolders = waitQueueManager.getQueueHoldersForWriteOperation(variableName);

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

    /**
     * Tries to perform a read for given transaction and variable. Returns the integer value of the required variable
     * or returns null if the variable is unavailable
     * @author Saumya
     */
    Optional<Integer> read(String transactionName, String variableName) {
        if (transactionMap.get(transactionName) instanceof ReadOnlyTransaction) {
            return readFromReadOnlyTransaction(transactionMap.get(transactionName), variableName);
        }

        ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transactionMap.get(transactionName);
        return readFromReadWriteTransaction(readWriteTransaction, variableName);
    }

    /**
     * Tries to perform a read for Read-Write transaction
     * @author Saumya
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
               return Optional.of(readWriteTransaction.getModifiedVariables().get(variableName));
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

        handleWaitingForOperation(variableName, transactionName);
        return Optional.empty();
    }

    /**
     * Tries to perform a read for Read-Only transaction
     * @author Omkar
     */
    private Optional<Integer> readFromReadOnlyTransaction(Transaction transaction, String variableName) {
        ReadOnlyTransaction readOnlyTransaction = (ReadOnlyTransaction) transaction;
        Long tickTime = readOnlyTransaction.getVariableTickTIme(variableName);
        Optional<Integer> val = siteManager.readForRO(variableName, tickTime);
        if (!val.isPresent()) {
            readOnlyTransaction.setPendingReadVariable(variableName);
        }
        return val;
    }

    /**
     * Checks if any Read only transactions are waiting for a particular site recovery and executes the reads if possible
     * @author Omkar
     */
    void checkROTransactionsForWaitingOperations(int siteId) {
        for (String transactionName : transactionMap.keySet()) {
            Transaction transaction = transactionMap.get(transactionName);
            if (transaction instanceof ReadOnlyTransaction) {
                ReadOnlyTransaction readOnlyTransaction = (ReadOnlyTransaction) transaction;
                if (readOnlyTransaction.getPendingReadVariable() != null) {
                    String variableName = readOnlyTransaction.getPendingReadVariable();
                    Long tickTime = readOnlyTransaction.getVariableTickTIme(variableName);
                    Optional<Integer> readValue = siteManager.readForROFromSpecificSite(variableName, tickTime, siteId);
                    readValue.ifPresent(integer -> System.out.println(variableName + ": " + integer));
                }
            }
        }
    }

    /**
     * Ends the given transaction and commits it if possible
     * @author Omkar
     */
    void endTransaction(String transactionName, long tickTime) {
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

    /**
     * Performs any waiting operations for given variable
     * @author Saumya
     */
    void processWaitingOperationsIfAny(String variableName) {
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
            }

        }
    }

    /**
     * Releases all locks and waiting operations for Read Write transaction
     * @author Saumya
     */
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

    /**
     * Commits the given transaction if possible
     * @author Saumya
     */
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

    /**
     * Checks if a transaction should be aborted because of failure of accessed site
     * @author Omkar
     */
    void checkTransactionsForAbortionAfterSiteFailure(int siteId) {
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
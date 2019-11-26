package nyu.edu.adb.project;

import javax.swing.text.html.Option;
import java.util.*;

public class TransactionManager {
    Map<String, Transaction> transactionMap;
    SiteManager siteManager;
    DataManager dataManager;

    TransactionManager(SiteManager siteManager, DataManager dataManager) {
        transactionMap = new HashMap<>();
        this.siteManager = siteManager;
        this.dataManager = dataManager;
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

    public void write(String transactionName, String variableName, int value) throws Exception {

        ReadWriteTransaction t;
        if (!(transactionMap.get(transactionName) instanceof ReadWriteTransaction)) {
            throw new IllegalArgumentException("Transaction " + transactionName + " is a ReadOnly Transaction, " +
                    "cannot write");
        }
        t = (ReadWriteTransaction)transactionMap.get(transactionName);

        if (t.getWriteLocks().contains(variableName)) {
            t.writeToVariable(variableName, value);
            return;
        }

        List<Integer> list = siteManager.getWriteLock(variableName);
        if (!list.isEmpty()) {
            t.addWriteLock(variableName, list);
            t.addAccessedSites(list);
            t.writeToVariable(variableName, value);
            return;
        }

        // Adding operation to wait queue in order to finish it in future
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

        if (readWriteTransaction.hasWriteLock(variableName)) {
            //TODO - change logic - if write lock present, read written value

            List<Integer> siteIdList = readWriteTransaction.getWriteLockSiteId(variableName);
            int siteId = siteIdList.get(0);
            data = siteManager.read(variableName, siteId);
            if (data.isPresent()) {
                return data;
            }
        }

        final int siteId = siteManager.getReadLock(variableName);

        if(siteId != -1) {
            readWriteTransaction.addReadLock(variableName, siteId);
            readWriteTransaction.addAccessedSite(siteId);
            return siteManager.read(variableName, siteId);
        }

        String transactionName = readWriteTransaction.getName();
        // Adding operation to wait queue in order to finish it in future
        dataManager.addWaitingOperation(variableName,
                new Operation(transactionName, Operation.OperationType.READ, variableName));
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
            Set<String> writeLockVariablesSet = readWriteTransaction.getWriteLocks();
            for(String writeLockVariable: writeLockVariablesSet) {
                List<Integer> siteIdList = readWriteTransaction.getWriteLockSiteId(writeLockVariable);
                for(Integer siteId: siteIdList) {
                    siteManager.releaseWriteLock(writeLockVariable, siteId);
                }
            }
        }

        if(transaction instanceof ReadWriteTransaction) {
            ReadWriteTransaction readWriteTransaction = (ReadWriteTransaction) transaction;
            Set<String> readLockVariablesSet = readWriteTransaction.getReadLocks();
            for(String readLockVariable: readLockVariablesSet) {
                int siteId = readWriteTransaction.getReadLockSiteId(readLockVariable);
                siteManager.releaseReadLock(readLockVariable, siteId);
            }
        }

        //TODO - remove transaction from waiting queues and graph

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

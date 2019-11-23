package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            t.addWriteLock(variableName);
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
        if (readWriteTransaction.hasReadLock(variableName)) {
            Optional<Integer> value = siteManager.read(variableName);
            return value;
        }
        return Optional.empty();
    }

    private int readFromReadOnlyTransaction(Transaction transaction, String variableName) {
        ReadOnlyTransaction readOnlyTransaction = (ReadOnlyTransaction)transaction;
        return readOnlyTransaction.getVariable(variableName);
    }

    public void endTransaction(String transactionName) {
        boolean wasCommitted = commitTransaction();
        transactionMap.remove(transactionName);
    }

    private boolean commitTransaction() {
        // TODO - finish the commit logic
        return false;
    }
}

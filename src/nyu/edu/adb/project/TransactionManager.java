package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.Map;

public class TransactionManager {
    Map<String, Transaction> transactionMap;
    LockManager lockManager;

    TransactionManager() {
        transactionMap = new HashMap<>();
        lockManager = new LockManager();
    }

    public void createTransaction(String transactionName, long tickTime) throws Exception {
        Transaction transaction = new Transaction(transactionName, tickTime);
        if (transactionMap.containsKey(transactionName)) {
            throw new Exception("Transaction with name " + transactionName + " already exists");
        }
        transactionMap.put(transactionName, transaction);
    }

    public void write(String transactionName, String variableName, int value) throws Exception {
        Transaction t = transactionMap.get(transactionName);
        if (t.getWriteLocks().contains(variableName) ||
                lockManager.getWriteLock(transactionName, variableName)) {
            t.addWriteLock(variableName);
            t.writeToVariable(variableName, value);
        } else {
            //TODO - if we fail to get writeLock, add the transaction/operation to appropriate wait queue
        }
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

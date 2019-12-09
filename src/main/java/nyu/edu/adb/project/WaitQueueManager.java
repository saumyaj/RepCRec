package nyu.edu.adb.project;

import java.util.*;
import java.util.stream.Collectors;

class WaitQueueManager {
    Map<String, List<Operation>> variableWaitQueueMap;
    List<String> listOfVariables;

    WaitQueueManager() {
        variableWaitQueueMap = new HashMap<>();
        listOfVariables = new ArrayList<>();
    }

    /**
     * Removes and returns the first waiting operation from the queue of a particular variable if any
     * @param transactionName name of the transaction
     * @author Saumya
     */
    void removeAllPendingOperationOfTransaction(String transactionName) {
        for (String variableName: variableWaitQueueMap.keySet()) {
            List<Operation> waitingOperations = variableWaitQueueMap.get(variableName);
            waitingOperations = waitingOperations.stream()
                    .filter(op -> !op.getTransactionId().equals(transactionName))
                    .collect(Collectors.toList());
            variableWaitQueueMap.put(variableName, waitingOperations);
        }
    }

    /**
     * Removes and returns the first waiting operation from the queue of a particular variable if any
     * @param variableName name of the variable
     * @return First waiting Operation object if any
     * @author Saumya
     */
    Optional<Operation> pollNextWaitingOperation(String variableName) {
        List<Operation> waitQueue = variableWaitQueueMap.getOrDefault(variableName, new ArrayList<>());
        if (!waitQueue.isEmpty()) {
            return Optional.of(waitQueue.remove(0));
        }
        return Optional.empty();
    }

    /**
     * Returns a list of all the operations from the queue until a write operation is found
     * @param variableName name of the variable
     * @return List of Operations until next write operation
     * @author Saumya
     */
    List<Operation> pollUntilNextWriteOperation(String variableName) {
        List<Operation> waitQueue = variableWaitQueueMap.getOrDefault(variableName, new ArrayList<>());
        List<Operation> readOperations = new ArrayList<>();
        while(!waitQueue.isEmpty()) {
            Operation operation = waitQueue.get(0);
            if (operation.getOperationType().equals(Operation.OperationType.WRITE)) {
                break;
            }
            waitQueue.remove(0);
            readOperations.add(operation);
        }
        return readOperations;
    }

    /**
     * Returns the first waiting operation without removing it from the queue of a particular variable if any
     * @param variableName name of the variable
     * @return Next waiting Operation in the queue if any
     * @author Saumya
     */
    Optional<Operation> peekAtNextWaitingOperation(String variableName) {
        List<Operation> waitQueue = variableWaitQueueMap.getOrDefault(variableName, new ArrayList<>());
        if (waitQueue.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(waitQueue.get(0));
    }

    /**
     * Returns true if a write operation is already waiting for the given variable
     * @param variableName name of the variable
     * @return true if a write operation is already waiting for the given variable, false otherwise
     * @author Saumya
     */
    boolean precedingWriteOperationExists(String variableName) {
        List<Operation> waitQueue = variableWaitQueueMap.getOrDefault(variableName, new ArrayList<>());
        for (Operation op: waitQueue) {
            if (op.getOperationType().equals(Operation.OperationType.WRITE)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if some operation is waiting for the given variable
     * @param variableName name of the variable
     * @return true if some operation is waiting for the given variable, false otherwise.
     * @author Saumya
     */
    boolean isOperationAlreadyWaiting(String variableName) {
        if (variableWaitQueueMap.containsKey(variableName)) {
            return !variableWaitQueueMap.get(variableName).isEmpty();
        }
        return false;
    }

    /**
     * Adds the given operation to the wait queue of the given variable
     * @param variableName name of the variable
     * @param operation Operation to be added
     * @author Saumya
     */
    void addWaitingOperation(String variableName, Operation operation) {
        List<Operation> waitQueue = variableWaitQueueMap
                .getOrDefault(variableName, new ArrayList<Operation>());
        waitQueue.add(operation);
        variableWaitQueueMap.put(variableName, waitQueue);
    }

    /**
     * Returns the id of the transaction that has the last waiting operation for the given variable if any
     * @param variableName name of the variable
     * @return the id of the transaction that has the last waiting operation for the given variable if any
     * @author Saumya
     */
    Optional<String> getLastWriteTransaction(String variableName) {

        List<Operation> list = variableWaitQueueMap.getOrDefault(variableName, new ArrayList<>());
        for(int i=list.size()-1; i>=0;i--) {
            Operation operation = list.get(i);
            if (operation.getOperationType().equals(Operation.OperationType.WRITE)) {
                return Optional.of(operation.getTransactionId());
            }
        }

        return Optional.empty();
    }

    /**
     * Returns all the transaction ids that has a waiting write operation on the given variable
     * @param variableName name of the variable
     * @return List of all the transaction ids that has a waiting write operation on the given variable
     * @author Saumya
     */
    List<String> getQueueHoldersForWriteOperation(String variableName) {
        List<String> queueHolders = new ArrayList<>();
        if (variableWaitQueueMap.containsKey(variableName)
                && variableWaitQueueMap.get(variableName).size()>0) {
            List<Operation> list = variableWaitQueueMap.get(variableName);
            Operation op = list.get(list.size()-1);
            if (op.getOperationType().equals(Operation.OperationType.WRITE)) {
                queueHolders.add(op.getTransactionId());
                return queueHolders;
            }
            for(int i=list.size()-1; i>=0;i--) {
                Operation operation = list.get(i);
                if (operation.getOperationType().equals(Operation.OperationType.WRITE)) {
                    break;
                }
                queueHolders.add(operation.getTransactionId());
            }
            return queueHolders;
        }
        return queueHolders;
    }
}

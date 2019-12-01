package nyu.edu.adb.project;

import java.util.*;
import java.util.stream.Collectors;

class DataManager {
    Map<String, List<Operation>> variableWaitQueueMap;
    List<String> listOfVariables;

    DataManager() {
        variableWaitQueueMap = new HashMap<>();
        listOfVariables = new ArrayList<>();
    }

    void removeAllPendingOperationOfTransaction(String transactionName) {
        for (String variableName: variableWaitQueueMap.keySet()) {
            List<Operation> waitingOperations = variableWaitQueueMap.get(variableName);
            waitingOperations = waitingOperations.stream()
                    .filter(op -> !op.getTransactionId().equals(transactionName))
                    .collect(Collectors.toList());
            variableWaitQueueMap.put(variableName, waitingOperations);
        }
    }

    Optional<Operation> pollNextWaitingOperation(String variableName) {
        List<Operation> waitQueue = variableWaitQueueMap.getOrDefault(variableName, new ArrayList<>());
        if (!waitQueue.isEmpty()) {
            return Optional.of(waitQueue.remove(0));
        }
        return Optional.empty();
    }

    // This function is only called if the first operation in the queue is a read operation
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

    Optional<Operation> peekAtNextWaitingOperation(String variableName) {
        List<Operation> waitQueue = variableWaitQueueMap.getOrDefault(variableName, new ArrayList<>());
        if (waitQueue.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(waitQueue.get(0));
    }

    boolean precedingWriteOperationExists(String variableName) {
        List<Operation> waitQueue = variableWaitQueueMap.getOrDefault(variableName, new ArrayList<>());
        for (Operation op: waitQueue) {
            if (op.getOperationType().equals(Operation.OperationType.WRITE)) {
                return true;
            }
        }
        return false;
    }

    boolean isOperationAlreadyWaiting(String variableName) {
        if (variableWaitQueueMap.containsKey(variableName)) {
            return !variableWaitQueueMap.get(variableName).isEmpty();
        }
        return false;
    }

    void addWaitingOperation(String variableName, Operation operation) {
        List<Operation> waitQueue = variableWaitQueueMap
                .getOrDefault(variableName, new ArrayList<Operation>());
        waitQueue.add(operation);
        variableWaitQueueMap.put(variableName, waitQueue);
    }

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

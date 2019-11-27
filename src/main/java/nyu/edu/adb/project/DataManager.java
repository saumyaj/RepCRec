package nyu.edu.adb.project;

import java.util.*;

class DataManager {
    Map<String, List<Operation>> variableWaitQueueMap;
    List<String> listOfVariables;

    DataManager() {
        variableWaitQueueMap = new HashMap<>();
        listOfVariables = new ArrayList<>();
    }

    Optional<Operation> getNextWaitingOperation(String variableName) {
        List<Operation> waitQueue = variableWaitQueueMap.get(variableName);
        if (!waitQueue.isEmpty()) {
            return Optional.of(waitQueue.remove(0));
        }
        return Optional.empty();
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
}

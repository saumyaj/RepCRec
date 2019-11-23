package nyu.edu.adb.project;

import java.util.*;

class DataManager {
    Map<String, Queue<Operation>> variableWaitQueueMap;
    List<String> listOfVariables;

    DataManager() {
        variableWaitQueueMap = new HashMap<>();
        listOfVariables = new ArrayList<>();
    }

    Optional<Operation> getNextWaitingOperation(String variableName) {
        Queue<Operation> waitQueue = variableWaitQueueMap.get(variableName);
        if (!waitQueue.isEmpty()) {
            return Optional.of(waitQueue.poll());
        }
        return Optional.empty();
    }

    void addWaitingOperation(String variableName, Operation operation) {
        Queue<Operation> waitQueue = variableWaitQueueMap
                .getOrDefault(variableName, new LinkedList<Operation>());
        waitQueue.add(operation);
        variableWaitQueueMap.put(variableName, waitQueue);
    }
}

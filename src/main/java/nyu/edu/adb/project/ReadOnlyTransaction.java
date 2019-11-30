package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.Map;

class ReadOnlyTransaction extends Transaction {
    Map<String, Integer> allVariablesData;
    Map<String, Long> lastWriteTimeMap;
    String pendingReadVariable;

    ReadOnlyTransaction(String id, long tickTime, Map<String, Long> lastWriteTimeMap) {
        super(id, tickTime);
        allVariablesData = new HashMap<>();
        this.lastWriteTimeMap = lastWriteTimeMap;
        pendingReadVariable = null;
    }

    Long getVariableTickTIme(String variableName) {
        return lastWriteTimeMap.get(variableName);
    }

    public String getPendingReadVariable() {
        return pendingReadVariable;
    }

    public void setPendingReadVariable(String pendingReadVariable) {
        this.pendingReadVariable = pendingReadVariable;
    }
}
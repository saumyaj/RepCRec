package nyu.edu.adb.project;

import java.util.Map;

class ReadOnlyTransaction extends Transaction {
    private Map<String, Long> lastWriteTimeMap;
    private String pendingReadVariable;

    ReadOnlyTransaction(String id, long tickTime, Map<String, Long> lastWriteTimeMap) {
        super(id, tickTime);
        this.lastWriteTimeMap = lastWriteTimeMap;
        pendingReadVariable = null;
    }


    /**
     * Obtains the tick time of the last committed write for given variable
     * @param variableName
     * @return the tick time
     * @author Omkar
     */
    Long getVariableTickTIme(String variableName) {
        return lastWriteTimeMap.get(variableName);
    }

    String getPendingReadVariable() {
        return pendingReadVariable;
    }

    void setPendingReadVariable(String pendingReadVariable) {
        this.pendingReadVariable = pendingReadVariable;
    }
}
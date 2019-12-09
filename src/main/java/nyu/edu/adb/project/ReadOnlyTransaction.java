package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.Map;

class ReadOnlyTransaction extends Transaction {
    Map<String, Long> lastWriteTimeMap;
    String pendingReadVariable;

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

    public String getPendingReadVariable() {
        return pendingReadVariable;
    }

    public void setPendingReadVariable(String pendingReadVariable) {
        this.pendingReadVariable = pendingReadVariable;
    }
}
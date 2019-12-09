package nyu.edu.adb.project;

/**
 * This class is just a data structure to hold the information about read/write operations
 * @author Saumya
 */
class Operation {
    enum OperationType {
        READ, WRITE
    }
    private final String transactionId;
    private final OperationType operationType;
    private final String variableName;
    private final Integer value;

    Operation(String transactionId, OperationType operationType, String variableName) {
        this.transactionId = transactionId;
        this.operationType = operationType;
        this.variableName = variableName;
        this.value = null;
    }

    Operation(String transactionId, OperationType operationType, String variableName, int value) {
        this.transactionId = transactionId;
        this.operationType = operationType;
        this.variableName = variableName;
        this.value = value;
    }

    String getTransactionId() {
        return transactionId;
    }

    OperationType getOperationType() {
        return operationType;
    }

    public String getVariableName() {
        return variableName;
    }

    Integer getValue() {
        return value;
    }
}

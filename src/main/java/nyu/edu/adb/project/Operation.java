package nyu.edu.adb.project;

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

    public String getTransactionId() {
        return transactionId;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public String getVariableName() {
        return variableName;
    }
}

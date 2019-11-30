package nyu.edu.adb.project;

import java.util.HashMap;
import java.util.Map;

class ReadOnlyTransaction extends Transaction {
    Map<String, Integer> allVariablesData;


    ReadOnlyTransaction(String id, long tickTime) {
        super(id, tickTime);
        allVariablesData = new HashMap<>();
        readAllData();
    }

    private void readAllData() {
        //TODO - This method reads most recently committed copy of all the data in the database
        
    }

    int getVariable(String variableName) {
        //TODO - change this method to return appropriate variable value
        return -1;
    }
}

package nyu.edu.adb.project;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;

class Transaction {
    String id;

    long beginTime;

    Transaction(String id, long tickTime) {
        this.id = id;
        beginTime = tickTime;
    }
}
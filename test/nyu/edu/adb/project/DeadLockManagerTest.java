package nyu.edu.adb.project;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DeadLockManagerTest {

    DeadLockManager deadLockManager = new DeadLockManager();

    @Test
    void testingCycleDetection() {
        deadLockManager.addEdge("1", "2");
        deadLockManager.addEdge("2", "3");
        deadLockManager.addEdge("3", "1");
        deadLockManager.addEdge("1", "4");
        deadLockManager.addEdge("4", "5");
        deadLockManager.addEdge("5", "6");
        deadLockManager.addEdge("6", "4");

        deadLockManager.hasCycle();
    }

}
package nyu.edu.adb.project;

import java.util.*;

class DeadLockManager {
    private Map<String, Set<String>> waitsForGraph;
    private Set<String> vertices;
    private List<List<String>> cycles;

    DeadLockManager() {
        waitsForGraph = new HashMap<>();
        vertices = new HashSet<>();
        cycles = new ArrayList<>();
    }

    /**
     * Adds edge between two transaction nodes in the waits-for graph
     * @param t1 Source transaction name
     * @param t2 Target transaction name
     * @author Saumya
     */
    void addEdge(String t1, String t2) {
        if (t1.equals(t2)) {
            return;
        }
        Set<String> neighbors = waitsForGraph.getOrDefault(t1, new HashSet<>());
        neighbors.add(t2);
        waitsForGraph.put(t1, neighbors);
        vertices.add(t1);
        vertices.add(t2);
    }

    /**
     * Adds multiple directed edges from the source transaction to the list of target transactions
     * @param src Source transaction
     * @param targets List of target transactions
     * @author Saumya
     */
    void addMultipleEdges(String src, List<String> targets) {
        for (String target: targets) {
            addEdge(src, target);
        }
    }

    /**
     * Removes the edge from t1 to t2
     * @param t1 Source transaction name
     * @param t2 Target transaction name
     * @author Saumya
     */
    void removeEdge(String t1, String t2) {
        Set<String> neighbors = waitsForGraph.getOrDefault(t1, new HashSet<>());
        neighbors.remove(t2);
        waitsForGraph.put(t1, neighbors);
    }

    /**
     * Removes a transaction from the waits-for graph
     * @param t1 transaction name
     * @author Saumya
     */
    void removeNode(String t1) {
        waitsForGraph.remove(t1);
        for (Set<String> set: waitsForGraph.values()) {
            set.remove(t1);
        }
    }

    /**
     * Runs the cycle detection algorithm on the waits for graph and returns a list of all the cycles
     * @return list of cycles represented as list of transaction names
     * @author Saumya
     */
    List<List<String>> getDeadLockCycles() {
        Set<String> whiteSet = new HashSet<>();
        Set<String> graySet = new HashSet<>();
        Set<String> blackSet = new HashSet<>();
        Map<String, String> parentMap = new HashMap<>();
        cycles.clear();

        for (String vertex : vertices) {
            whiteSet.add(vertex);
        }

        while (whiteSet.size() > 0) {
            String current = whiteSet.iterator().next();
            parentMap.put(current, null);
            dfs(current, whiteSet, graySet, blackSet, parentMap);
        }
        return cycles;
    }

    /**
     * DFS algorithm as part of the cycle detection algorithm
     * @author Saumya
     */
    private void dfs(String current, Set<String> whiteSet,
                        Set<String> graySet, Set<String> blackSet, Map<String, String> parentMap ) {
        //move current to gray set from white set and then explore it.
        moveVertex(current, whiteSet, graySet);
        for(String neighbor : waitsForGraph.getOrDefault(current, new HashSet<>())) {
            //if in black set means already explored so continue.
            if (blackSet.contains(neighbor)) {
                continue;
            }
            //if in gray set then cycle found.
            if (graySet.contains(neighbor)) {
                traceCycle(neighbor, current, parentMap);
                return;
            }
            parentMap.put(neighbor, current);
            dfs(neighbor, whiteSet, graySet, blackSet, parentMap);
        }
        //move vertex from gray set to black set when done exploring.
        moveVertex(current, graySet, blackSet);
    }

    /**
     * Traces back the nodes in the cycle when the cycle is detected
     * @author Saumya
     */
    private void traceCycle(String lastNode, String secondLastNode, Map<String, String> parentMap) {
        List<String> cycle = new ArrayList<>();
        cycle.add(lastNode);
        String parent = secondLastNode;
        while(true) {
            if (cycle.contains(parent))
                break;
            cycle.add(parent);
            parent = parentMap.get(parent);
        }
        cycles.add(cycle);
    }

    /**
     * A helper function for the cycle detection algorithm
     * @author Saumya
     */
    private void moveVertex(String vertex, Set<String> sourceSet,
                            Set<String> destinationSet) {
        sourceSet.remove(vertex);
        destinationSet.add(vertex);
    }
}

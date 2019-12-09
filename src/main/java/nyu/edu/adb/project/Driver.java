package nyu.edu.adb.project;

import java.io.File;
import java.util.List;
import java.util.Scanner;

public class Driver {
    public static void main(String[] args) throws Exception {
        for(String arg: args) {
            executeFromFile(arg);
        }
    }

    /**
     * Executes the instructions from given file in a database
     * @param filename name of the file
     * @author Omkar
     */
    public static void executeFromFile(String filename) throws Exception {
        Scanner sc = new Scanner(new File(filename));
        Database database = new Database();
        while(sc.hasNextLine()) {
            String line = sc.nextLine();
            database.handleQuery(line);
        }
//        database.dump();
    }

    /**
     * Executes list of instructions in a new database
     * @param instructions The list to execute
     * @throws Exception
     * @author Omkar
     */
    public static void executeFromList(List<String> instructions) throws Exception {
        Database database = new Database();
        for (String s: instructions) {
            database.handleQuery(s);
        }
    }

    /**
     * Executes list of instructions in given database
     * @param instructions The list to execute
     * @throws Exception
     * @author Omkar
     */
    public static void executeFromList(Database database, List<String> instructions) throws Exception {
        for (String s: instructions) {
            database.handleQuery(s);
        }
    }

}
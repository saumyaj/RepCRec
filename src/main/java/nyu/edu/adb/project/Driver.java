package nyu.edu.adb.project;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.List;
import java.util.Scanner;

public class Driver {
    public static void main(String[] args) throws Exception {
//        Driver driver = new Driver();
        executeFromFile("tests.txt");
    }

    public static void executeFromFile(String filename) throws Exception {
        Scanner sc = new Scanner(new File(filename));
        Database database = new Database();
        while(sc.hasNextLine()) {
            String line = sc.nextLine();
            database.handleQuery(line);
        }
        database.dump();
    }

    public static void executeFromList(List<String> instructions) throws Exception {
        Database database = new Database();
        for (String s: instructions) {
            database.handleQuery(s);
        }
    }

    public static void executeFromList(Database database, List<String> instructions) throws Exception {
        for (String s: instructions) {
            database.handleQuery(s);
        }
    }

}
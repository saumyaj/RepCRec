package nyu.edu.adb.project;

import java.io.File;
import java.util.Scanner;

public class Driver {
    public static void main(String[] args) throws Exception {
        Driver driver = new Driver();
        driver.executeFromFile("tests.txt");
    }

    public void executeFromFile(String filename) throws Exception {
        Scanner sc = new Scanner(new File(filename));
        Database database = new Database();
        database.initialize();
        while(sc.hasNextLine()) {
            String line = sc.nextLine();
            database.handleQuery(line);
        }
        database.dump();
    }
}
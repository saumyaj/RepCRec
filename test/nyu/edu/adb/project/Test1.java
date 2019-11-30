package nyu.edu.adb.project;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class Test1 {

    private ByteArrayOutputStream baos;

    @BeforeEach
    void setUp() {
        baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));
    }

    @Test
    void testReadAfterWrite() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("W(T1, x1, 69)");
        instructions.add("R(T1, x1)");
        instructions.add("end(T1)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("69", lines[0]);
    }

    @Test
    void testReadAfterWriteMultipleTimes() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("W(T1, x1, 69)");
        instructions.add("R(T1, x1)");
        instructions.add("W(T1, x1, 77)");
        instructions.add("R(T1, x1)");
        instructions.add("W(T1, x1, 25)");
        instructions.add("R(T1, x1)");
        instructions.add("end(T1)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("69", lines[0]);
        assertEquals("77", lines[1]);
        assertEquals("25", lines[2]);
    }

    @Test
    void testMultipleReadAndWrites() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("R(T1, x1)");
        instructions.add("W(T1, x1, 69)");
        instructions.add("W(T1, x1, 70)");
        instructions.add("R(T1, x1)");
        instructions.add("R(T1, x1)");
        instructions.add("end(T1)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("10", lines[0]);
        assertEquals("70", lines[1]);
        assertEquals("70", lines[2]);
    }

    @Test
    void testReadAfterFailureForUnreplicatedVariable() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("fail(2)");
        instructions.add("R(T1, x1)");
        instructions.add("recover(2)");
        instructions.add("end(T1)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("10", lines[0]);
    }

    @Test
    void testReadAfterFailureForReplicatedVariable() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("fail(1)");
        instructions.add("R(T1, x2)");
        instructions.add("recover(1)");
        instructions.add("end(T1)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("20", lines[0]);
    }

    @Test
    void testFailureOfAccessedSiteAfterRead() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("R(T1, x2)");
        instructions.add("fail(1)");
        instructions.add("end(T1)");

        Database database = new Database();
        Driver.executeFromList(database, instructions);

        String[] lines = baos.toString().split("\n");
        assertEquals("20", lines[0]);
        assertTrue(database.transactionManager.abortedTransactions.contains("T1"));
    }

    @Test
    void testFailureOfUnAccessedSiteAfterRead() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("R(T1, x2)");
        instructions.add("fail(3)");
        instructions.add("end(T1)");

        Database database = new Database();
        Driver.executeFromList(database, instructions);

        String[] lines = baos.toString().split("\n");
        assertEquals("20", lines[0]);
        assertFalse(database.transactionManager.abortedTransactions.contains("T1"));
    }

    @Test
    void testFailureOfAccessedSiteAfterWrite() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T1, x2, 1000)");
        instructions.add("fail(3)");
        instructions.add("end(T1)");
        instructions.add("R(T2, x2)");
        instructions.add("end(T2)");

        Database database = new Database();
        Driver.executeFromList(database, instructions);

        String[] lines = baos.toString().split("\n");
        assertEquals("20", lines[0]);
        assertTrue(database.transactionManager.abortedTransactions.contains("T1"));
    }

    @Test
    void testFailureOfUnAccessedSiteAfterWrite() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("W(T1, x1, 1000)");
        instructions.add("fail(3)");
        instructions.add("end(T1)");

        Database database = new Database();
        Driver.executeFromList(database, instructions);

        String[] lines = baos.toString().split("\n");
        assertFalse(database.transactionManager.abortedTransactions.contains("T1"));
    }

    @Test
    void testReadCommittedValue() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T1, x1, 1000)");
        instructions.add("end(T1)");
        instructions.add("R(T2, x1)");
        instructions.add("end(T2)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("1000", lines[0]);
    }

    @Test
    void testReadCommittedValueAfterWaiting() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T1, x1, 1000)");
        instructions.add("R(T2, x1)");
        instructions.add("end(T1)");
        instructions.add("end(T2)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("1000", lines[0]);
    }

    @Test
    void testReadOnlyVariableVersion() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("beginRO(T2)");
        instructions.add("W(T1, x1, 1000)");
        instructions.add("end(T1)");
        instructions.add("R(T2, x1)");
        instructions.add("end(T2)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("10", lines[0]);
    }

    @Test
    void testReadOnlyIgnoresLocks() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("beginRO(T2)");
        instructions.add("begin(T3)");
        instructions.add("W(T1, x1, 1000)");
        instructions.add("end(T1)");
        instructions.add("W(T3, x1, 3000)");
        instructions.add("R(T2, x1)");
        instructions.add("end(T2)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("10", lines[0]);
    }

    @Test
    void testReadOnlyCommittedValueAfterWaiting() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("fail(2)");
        instructions.add("begin(T2)");
        instructions.add("beginRO(T1)");
        instructions.add("R(T1, x1)");
        instructions.add("R(T2, x2)");
        instructions.add("recover(2)");
        instructions.add("end(T1)");
        instructions.add("end(T2)");
        Driver.executeFromList(instructions);
        String[] lines = baos.toString().split("\n");
        assertEquals("20", lines[0]);
        assertEquals("10", lines[1]);
    }

    @Test
    void testingTest() throws Exception {
        List<String> instructions = new ArrayList<>();

        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
//        instructions.add("begin(T3)");
        instructions.add("R(T1, x1)");
        instructions.add("W(T2, x1, -1)");
        instructions.add("end(T1)");
//        instructions.add("R(T3, x1)");
        instructions.add("R(T2, x1)");
//        instructions.add("W(T3, x1, -2)");
//        instructions.add("fail(1)");
//        instructions.add("recover(1)");
//        instructions.add("end(T3)");
        instructions.add("end(T2)");
        instructions.add("dump()");

        Driver.executeFromList(instructions);

        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = baos.toString().split("\n");

//        System.out.println(baos);
        assertEquals("10", lines[0]);
        assertEquals("-1", lines[1]);


    }

}

package nyu.edu.adb.project;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class Test1 {

    private ByteArrayOutputStream baos;

    @BeforeEach
    void setUp() {
        baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));
    }

    //remove all outputs which are not reads
    String[] filterLines(String[] lines) {
        ArrayList<String> filteredLines = new ArrayList();
        for(String line: lines) {
            if(line.startsWith("x")) {
                filteredLines.add(line);
            }
        }

        String[] filteredLinesArray = new String[filteredLines.size()];
        for(int i = 0; i<filteredLinesArray.length; i++) {
            filteredLinesArray[i] = filteredLines.get(i);
        }
        return filteredLinesArray;
    }

    @Test
    void testReadAfterWrite() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("W(T1, x1, 69)");
        instructions.add("R(T1, x1)");
        instructions.add("end(T1)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 69", lines[0]);
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
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 69", lines[0]);
        assertEquals("x1: 77", lines[1]);
        assertEquals("x1: 25", lines[2]);
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
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 10", lines[0]);
        assertEquals("x1: 70", lines[1]);
        assertEquals("x1: 70", lines[2]);
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
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 10", lines[0]);
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
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x2: 20", lines[0]);
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

        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x2: 20", lines[0]);
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

        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x2: 20", lines[0]);
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

        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x2: 20", lines[0]);
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

        String[] lines = filterLines(baos.toString().split("\n"));
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
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 1000", lines[0]);
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
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 1000", lines[0]);
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
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 10", lines[0]);
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
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 10", lines[0]);
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
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x2: 20", lines[0]);
        assertEquals("x1: 10", lines[1]);
    }

    @Test
    void testReadOnlyNoAbortion() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("beginRO(T2)");
        instructions.add("W(T1, x1, 1000)");
        instructions.add("R(T2, x2)");
        instructions.add("W(T1, x2, 2000)");
        instructions.add("R(T2, x1)");
        instructions.add("end(T1)");
        instructions.add("end(T2)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x2: 20", lines[0]);
        assertEquals("x1: 10", lines[1]);
    }

    @Test
    void testDeadlockAbortion() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T1, x1, 1000)");
        instructions.add("W(T2, x2, 2000)");
        instructions.add("W(T1, x2, 1001)");
        instructions.add("W(T2, x1, 2001)");
        instructions.add("end(T1)");
        instructions.add("begin(T3)");
        instructions.add("R(T3, x1)");
        instructions.add("R(T3, x2)");
        instructions.add("end(T3)");
//        instructions.add("end(T2)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 1000", lines[0]);
        assertEquals("x2: 1001", lines[1]);
//        assertEquals("10", lines[1]);
    }

    // TODO - Rename this to what it is testing
    @Test
    void testWaitingAndReadingWrittenValue() throws Exception {
        List<String> instructions = new ArrayList<>();

        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1, x1)");
        instructions.add("W(T2, x1, -1)");
        instructions.add("end(T1)");
        instructions.add("R(T2, x1)");
        instructions.add("end(T2)");
        instructions.add("dump()");

        Driver.executeFromList(instructions);

        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

        System.out.println(Arrays.toString(lines));
        assertEquals("x1: 10", lines[0]);
        assertEquals("x1: -1", lines[1]);
    }

    @Test
    void testNoAbortForReadOnly() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("beginRO(T2)");
        instructions.add("W(T1,x1,101)");
        instructions.add("R(T2,x2)");
        instructions.add("W(T1,x2,102)");
        instructions.add("R(T2,x1)");
        instructions.add("end(T1)");
        instructions.add("end(T2)");
        Driver.executeFromList(instructions);
//        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

        assertEquals("x2: 20", lines[0]);
        assertEquals("x1: 10", lines[1]);
    }

    // T1 should not abort because its site did not fail.
    // In fact all transactions commit
    // x8 has the value 88 at every site except site 2 where it won't have
    // the correct value right away but must wait for a write to take place.
    @Test
    void testNoAbortIfNoRelevantSiteFails() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x3)");
        instructions.add("fail(2)");
        instructions.add("W(T2,x8,88)");
        instructions.add("R(T2,x3)");
        instructions.add("W(T1, x5,91)");
        instructions.add("R(T1, x8)");
        instructions.add("end(T2)");
        instructions.add("recover(2)");
        instructions.add("end(T1)");
        Driver.executeFromList(instructions);
//        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

        assertEquals("x3: 30", lines[0]);
        assertEquals("x3: 30", lines[1]);
        assertEquals("x8: 88", lines[2]);
    }


    // T1 should not abort because site 4 did not fail.
    // However T1 will write to x4 on every site except site 2.
    // Site 2 should not be able to respond to read requests for any
    // replicated variable after it recovers until a write is committed to it.
    // T1's write will not go to site 2, so every site except site 2
    // will have x4 equal to 91
    // x8 will not value 88 because T2 aborts
    // the correct value right away but must wait for a write to take place.
    // So W(T2,x8,88)
    // will not commit and is lost on failure.
    // Even though site 2 recovers before T2, T2 will not retroactively
    // write to the site (in any practical version of available copies).
    // T2 aborts because it wrote to x8.
    @Test
    void testNoAbortIfNoFail() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x3)");
        instructions.add("W(T2,x8,88)");
        instructions.add("fail(2)");
        instructions.add("R(T2,x3)");
        instructions.add("W(T1, x4,91)");
        instructions.add("recover(2)");
        instructions.add("end(T2)");
        instructions.add("end(T1)");
        instructions.add("begin(T3)");
        instructions.add("R(T3,x8)");
        instructions.add("R(T3,x4)");
        instructions.add("end(T3)");


        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x3: 30", lines[0]);
        assertEquals("x3: 30", lines[1]);
        assertEquals("x8: 80", lines[2]);
        assertEquals("x4: 91", lines[3]);
    }

    // T1 should not abort because site 4 did not fail.
    // In this case, T1 will write to x4 on every site.
    // x8 will not value 88 because T2 aborts
    // the correct value right away but must wait for a write to take place.
    // So W(T2,x8,88)
    // will not commit and is lost on failure.
    // Even though site 2 recovers before T2, T2 will not retroactively
    // write to the site (in any practical version of available copies).
    // T2 aborts because it wrote to x8.
    @Test
    void testAbortIfAccessedSiteFails() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x3)");
        instructions.add("W(T2,x8,88)");
        instructions.add("fail(2)");
        instructions.add("R(T2,x3)");
        instructions.add("recover(2)");
        instructions.add("W(T1, x4,91)");

        instructions.add("end(T2)");
        instructions.add("end(T1)");
        instructions.add("begin(T3)");
        instructions.add("R(T3,x8)");
        instructions.add("R(T3,x4)");
        instructions.add("end(T3)");


        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x3: 30", lines[0]);
        assertEquals("x3: 30", lines[1]);
        assertEquals("x8: 80", lines[2]);
        assertEquals("x4: 91", lines[3]);
    }

    // Now T1 aborts, since site 2 died after T1 accessed it. T2 ok.
    // Normally, we wait till the end(T1) to abort T1.
    // However, it is ok to abort T1 right away when fail(2) happens. Both
    // are correct.
    @Test
    void failIfReadAccessedSiteFails() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x1)");
        instructions.add("fail(2)");
        instructions.add("W(T2,x8,88)");
        instructions.add("R(T2,x3)");
        instructions.add("R(T1, x8)");
        instructions.add("end(T2)");
        instructions.add("recover(2)");
        instructions.add("end(T1)");

        Driver.executeFromList(instructions);
//        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x1: 10", lines[0]);
        assertEquals("x3: 30", lines[1]);
        assertEquals("x8: 88", lines[2]);
    }

    // T1 fails again here because it wrote to a site that failed. T2 ok.
    @Test
    void failIfWriteAccessedSiteFails() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T1,x6,66)");
        instructions.add("fail(2)");
        instructions.add("W(T2,x8,88)");
        instructions.add("R(T2,x3)");
        instructions.add("R(T1, x1)");
        instructions.add("end(T2)");
        instructions.add("recover(2)");
        instructions.add("end(T1)");

        instructions.add("begin(T3)");
        instructions.add("R(T3,x6)");
        instructions.add("end(T3)");

        Driver.executeFromList(instructions);
//        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x3: 30", lines[0]);
        assertEquals("x1: 10", lines[1]);
        assertEquals("x6: 60", lines[2]);
    }


    // T1 ok. T2 ok. T2 reads from a recovering site, but odd variables only
    // at that site
    // At the dump, sites 3 and 4 would have their original values for x8.
    // Future reads of x8 to those sites should be refused until a committed write
    // takes place.
    @Test
    void testOkToReadSafeVariables() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("fail(3)");
        instructions.add("fail(4)");
        instructions.add("R(T1, x1)");
        instructions.add("W(T2,x8,88)");
        instructions.add("end(T1)");
        instructions.add("recover(4)");
        instructions.add("recover(3)");
        instructions.add("R(T2,x3)");
        instructions.add("end(T2)");

        Driver.executeFromList(instructions);
//        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));
        assertEquals("x1: 10", lines[0]);
        assertEquals("x3: 30", lines[1]);
    }

    // T2 should read the initial version of x3 based on multiversion read
    // consistency.
    @Test
    void testMVC() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("beginRO(T2)");
        instructions.add("R(T2, x1)");
        instructions.add("R(T2, x2)");
        instructions.add("W(T1,x3,33)");
        instructions.add("end(T1)");
        instructions.add("R(T2,x3)");
        instructions.add("end(T2)");

        instructions.add("begin(T3)");
        instructions.add("R(T3,x3)");
        instructions.add("end(T3)");


        Driver.executeFromList(instructions);
//        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x1: 10", lines[0]);
        assertEquals("x2: 20", lines[1]);
        assertEquals("x3: 30", lines[2]);
        assertEquals("x3: 33", lines[3]);
    }


    // T1, T2, T3 ok. T3 waits and then complete after T2 commits
    @Test
    void testWaitingDequeue() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T3)");
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T3,x2,22)");
        instructions.add("W(T2,x4,44)");
        instructions.add("R(T3, x4)");
        instructions.add("end(T2)");
        instructions.add("end(T3)");
        instructions.add("R(T1, x2)");
        instructions.add("end(T1)");


        Driver.executeFromList(instructions);
//        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x4: 44", lines[0]);
        assertEquals("x2: 22", lines[1]);
    }

    // T3 should wait and should not abort
    @Test
    void testWaitIfLockNotAvailable() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("begin(T3)");
        instructions.add("W(T3,x2,22)");
        instructions.add("W(T2,x4,44)");
        instructions.add("R(T3,x4)");
        instructions.add("end(T2)");
        instructions.add("end(T3)");
        instructions.add("R(T1,x2)");
        instructions.add("end(T1)");


        Driver.executeFromList(instructions);
//        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x4: 44", lines[0]);
        assertEquals("x2: 22", lines[1]);
    }

    // All should commit
    @Test
    void testNoWaitRequireThenCommit() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x2)");
        instructions.add("R(T2,x2)");
        instructions.add("W(T2,x2,22)");
        instructions.add("end(T1)");
        instructions.add("R(T2,x2)");
        instructions.add("end(T2)");

        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

        System.out.println(Arrays.toString(lines));

        assertEquals("x2: 20", lines[0]);
        assertEquals("x2: 20", lines[1]);
        assertEquals("x2: 22", lines[2]);
    }

    // both commit
    @Test
    void testReadLockReleasedAfterCommitFinish() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x2)");
        instructions.add("R(T2,x2)");
        instructions.add("end(T1)");
        instructions.add("W(T2,x2,10)");
        instructions.add("end(T2)");
        instructions.add("begin(T3)");
        instructions.add("R(T3,x2)");
        instructions.add("end(T3)");


        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(filterLines(baos.toString().split("\n")));

        System.out.println(Arrays.toString(lines));

        assertEquals("x2: 20", lines[0]);
        assertEquals("x2: 20", lines[1]);
        assertEquals("x2: 10", lines[2]);
    }

    // T1 and T2 wait but eventually commit
    @Test
    void testWaitAndThenCommit() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("begin(T3)");
        instructions.add("begin(T4)");
        instructions.add("W(T3,x2,1)");
        instructions.add("W(T2,x2,2)");
        instructions.add("W(T1,x2,3)");
        instructions.add("end(T3)");
        instructions.add("R(T4, x2)");
        instructions.add("end(T2)");
        instructions.add("end(T1)");
        instructions.add("end(T4)");

        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(filterLines(baos.toString().split("\n")));

        System.out.println(Arrays.toString(lines));

        assertEquals("x2: 3", lines[0]);
    }


    // T1 will abort because x4 is on site 2 and  so
    // site 2 will lose its locks in the fail event.
    // So T1 will abort. T2 will be fine as will the others.
    @Test
    void testAbortIfAccessedSiteFails2() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T5)");
        instructions.add("begin(T4)");
        instructions.add("begin(T3)");
        instructions.add("begin(T2)");
        instructions.add("begin(T1)");
        instructions.add("W(T1,x4, 5)");
        instructions.add("fail(2)");
        instructions.add("W(T2,x4,44)");
        instructions.add("recover(2)");
        instructions.add("W(T3,x4,55)");
        instructions.add("W(T4,x4,66)");
        instructions.add("W(T5,x4,77)");
        instructions.add("end(T1)");
        instructions.add("end(T2)");
        instructions.add("end(T3)");
        instructions.add("end(T4)");
        instructions.add("end(T5)");

        instructions.add("begin(T9)");
        instructions.add("R(T9,x4)");
        instructions.add("end(T9)");

        Driver.executeFromList(instructions);
//        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(filterLines(baos.toString().split("\n")));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x4: 77", lines[0]);
    }


    @Test
    void testWaitAndThenReadFromTheLastCommittedValue() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T3)");
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T3,x2,22)");
        instructions.add("W(T2,x4,44)");
        instructions.add("R(T3,x4)");
        instructions.add("end(T2)");
        instructions.add("end(T3)");
        instructions.add("R(T1,x2)");
        instructions.add("end(T1)");

        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(filterLines(baos.toString().split("\n")));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x4: 44", lines[0]);
        assertEquals("x2: 22", lines[1]);

    }


    // T3 must wait till the commit of T2 before it reads x4
    // (because of locking), so sees 44.
    // T3 must abort though because the lock information is lost on site 4
    // upon failure
    // T1 reads the initial value of x2 because T3 has aborted.
    @Test
    void testCommitAbortsIfAccessedSiteFails() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T3)");
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T3,x2,22)");
        instructions.add("W(T2,x3,44)");
        instructions.add("R(T3,x3)");
        instructions.add("end(T2)");
        instructions.add("fail(4)");
        instructions.add("end(T3)");
        instructions.add("R(T1,x2)");
        instructions.add("end(T1)");

        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x3: 44", lines[0]);
        assertEquals("x2: 20", lines[1]);

    }

    @Test
    void testCircularDeadLock() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("begin(T3)");
        instructions.add("begin(T4)");
        instructions.add("begin(T5)");
        instructions.add("R(T3,x3)");
        instructions.add("R(T4,x4)");
        instructions.add("R(T5,x5)");
        instructions.add("R(T1,x1)");
        instructions.add("R(T2,x2)");
        instructions.add("W(T1,x2,22)");
        instructions.add("W(T2,x3,33)");
        instructions.add("W(T3,x4,44)");
        instructions.add("W(T4,x5,55)");
        instructions.add("W(T5,x1,11)");
        instructions.add("end(T4)");
        instructions.add("end(T3)");
        instructions.add("end(T2)");
        instructions.add("end(T1)");

        instructions.add("begin(T9)");
        instructions.add("R(T9,x1)");
        instructions.add("R(T9,x2)");
        instructions.add("R(T9,x3)");
        instructions.add("R(T9,x4)");
        instructions.add("R(T9,x5)");
        instructions.add("end(T9)");


        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x3: 30", lines[0]);
        assertEquals("x4: 40", lines[1]);
        assertEquals("x5: 50", lines[2]);
        assertEquals("x1: 10", lines[3]);
        assertEquals("x2: 20", lines[4]);
        assertEquals("x1: 10", lines[5]);
        assertEquals("x2: 22", lines[6]);
        assertEquals("x3: 33", lines[7]);
        assertEquals("x4: 44", lines[8]);
        assertEquals("x5: 55", lines[9]);
    }


    // An almost circular deadlock scenario with failures.
    // T3 fails (T2 and T4 do not fail because the site is up when they execute)
    // because site 4 fails.
    // All others succeed.
    @Test
    void testAlmostCircularScenarioWithFailures() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("begin(T3)");
        instructions.add("begin(T4)");
        instructions.add("begin(T5)");
        instructions.add("R(T3,x3)");
        instructions.add("fail(4)");
        instructions.add("recover(4)");
        instructions.add("R(T4,x4)");
        instructions.add("R(T5,x5)");
        instructions.add("R(T1,x6)");
        instructions.add("R(T2,x2)");
        instructions.add("W(T1,x2,22)");
        instructions.add("W(T2,x3,33)");
        instructions.add("W(T3,x4,44)");
        instructions.add("W(T5,x1,11)");
        instructions.add("end(T5)");
        instructions.add("W(T4,x5,55)");
        instructions.add("end(T4)");
        instructions.add("end(T3)");
        instructions.add("end(T2)");
        instructions.add("end(T1)");

        instructions.add("begin(T9)");
        instructions.add("R(T9,x1)");
        instructions.add("R(T9,x2)");
        instructions.add("R(T9,x3)");
        instructions.add("R(T9,x4)");
        instructions.add("R(T9,x5)");
        instructions.add("end(T9)");


        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x1: 11", lines[5]);
        assertEquals("x2: 22", lines[6]);
        assertEquals("x3: 33", lines[7]);
        assertEquals("x4: 40", lines[8]);
        assertEquals("x5: 55", lines[9]);

    }

    // T2 can't read x2 from site 1, so doesn't get a lock on x2 at site 1
    // T5 doesn't need to wait because T2 doesn't hold a lock since site 1 can't respond to the read.
    @Test
    void testWriteOperationSkipsAheadOfReadIfNoValidCopyAvailable() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T1,x2,9)");
        instructions.add("fail(1)");
        instructions.add("end(T1)");
        instructions.add("begin(T3)");
        instructions.add("W(T3,x2,100)");
        instructions.add("end(T3)");
        instructions.add("recover(1)");
        instructions.add("fail(2)");
        instructions.add("fail(3)");
        instructions.add("fail(4)");
        instructions.add("fail(5)");
        instructions.add("fail(6)");
        instructions.add("fail(7)");
        instructions.add("fail(8)");
        instructions.add("fail(9)");
        instructions.add("fail(10)");
        instructions.add("R(T2,x2)");
        instructions.add("begin(T5)");
        instructions.add("W(T5,x2,90)");
        instructions.add("end(T5)");
        instructions.add("end(T2)");

        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x2: 90", lines[0]);
//        assertEquals("20", lines[1]);

    }

    // T2 will try to promote its read lock to a write lock but can't
    // So there is a deadlock. T2 is younger so will abort.
    @Test
    void test() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T2, x2)");
        instructions.add("W(T1, x2, 202)");
        instructions.add("W(T2, x2, 302)");
        instructions.add("end(T1)");

        instructions.add("begin(T4)");
        instructions.add("R(T4, x2)");
        instructions.add("end(T4)");


        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));

//        System.out.println(Arrays.toString(lines));

        assertEquals("x2: 20", lines[0]);
        assertEquals("x2: 202", lines[1]);

    }

    // T2 can't read x2 from site 1, so doesn't get a lock on x2 at site 1
    // T5 doesn't need to wait because T2 doesn't hold a lock since site 1 can't respond to the read.
    @Test
    void testDeadlockAbortionWithWaiting() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("begin(T3)");
        instructions.add("W(T1,x1,5)");
        instructions.add("W(T3,x2,32)");
        instructions.add("W(T2,x1,17)");
        instructions.add("end(T1)");
        instructions.add("begin(T4)");
        instructions.add("W(T4,x4,35)");
        instructions.add("W(T3,x5,21)");
        instructions.add("W(T4,x2,21)");
        instructions.add("W(T3,x4,23)");
        instructions.add("end(T2)");
        instructions.add("end(T3)");
        instructions.add("end(T4)");
        instructions.add("dump()");

        Database database = new Database();
        Driver.executeFromList(database, instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));
        System.out.println(Arrays.toString(lines));
        assertTrue(database.transactionManager.abortedTransactions.contains("T4"));
    }

    // T2 should abort, T1 should not, because of wait-die
    // Younger T2 aborts.
    @Test
    void testYoungestTransactionAbortsAndWriteValuesAreDiscarded() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T1,x1,101)");
        instructions.add("W(T2,x2,202)");
        instructions.add("W(T1,x2,102)");
        instructions.add("W(T2,x1,201)");
        instructions.add("end(T1)");
        instructions.add("begin(T3)");
        instructions.add("R(T3, x1)");
        instructions.add("R(T3, x2)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x1: 101", lines[0]);
        assertEquals("x2: 102", lines[1]);
    }

    // No aborts happens, since read-only transactions use
    // multiversion read protocol.
    @Test
    void testReadOnlyTransactionNotAbortedForReadConflict() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("beginRO(T2)");
        instructions.add("W(T1,x1,101)");
        instructions.add("R(T2,x2)");
        instructions.add("W(T1,x2,102)");
        instructions.add("R(T2,x1)");
        instructions.add("end(T1)");
        instructions.add("end(T2)");

        instructions.add("begin(T3)");
        instructions.add("R(T3, x1)");
        instructions.add("R(T3, x2)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x2: 20", lines[0]);
        assertEquals("x1: 10", lines[1]);
        assertEquals("x1: 101", lines[2]);
        assertEquals("x2: 102", lines[3]);
    }


    // T1 should not abort because its site did not fail.
    // In fact all transactions commit
    @Test
    void testNoTransactionAbortedIfNoAccessedSiteFail() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x3)");
        instructions.add("fail(2)");
        instructions.add("W(T2,x8,88)");
        instructions.add("R(T2,x3)");
        instructions.add("W(T1, x5,91)");
        instructions.add("end(T2)");
        instructions.add("recover(2)");
        instructions.add("end(T1)");

        instructions.add("begin(T3)");
        instructions.add("R(T3, x8)");
        instructions.add("R(T3, x5)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x3: 30", lines[0]);
        assertEquals("x3: 30", lines[1]);
        assertEquals("x8: 88", lines[2]);
        assertEquals("x5: 91", lines[3]);
    }

    // Now T1 aborts, since site 2 died after T1 accessed it. T2 ok.
    // Normally, we wait till the end(T1) to abort T1.
    // However, it is ok to abort T1 right away when fail(2) happens. Both
    // are correct.
    @Test
    void testAbortWhenReadFromSiteFails() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x1)");
        instructions.add("fail(2)");
        instructions.add("W(T2,x8,88)");
        instructions.add("R(T2,x3)");
        instructions.add("R(T1, x5)");
        instructions.add("end(T2)");
        instructions.add("recover(2)");
        instructions.add("end(T1)");

        instructions.add("begin(T3)");
        instructions.add("R(T3, x8)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));

        assertEquals("x1: 10", lines[0]);
        assertEquals("x3: 30", lines[1]);
        assertEquals("x5: 50", lines[2]);
        assertEquals("x8: 88", lines[3]);
    }

    // T1 fails again here because it wrote to a site that failed. T2 ok.
    @Test
    void testAbortedWhenWrittenToSiteFails() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T1,x6,66)");
        instructions.add("fail(2)");
        instructions.add("W(T2,x8,88)");
        instructions.add("R(T2,x3)");
        instructions.add("R(T1, x5)");
        instructions.add("end(T2)");
        instructions.add("recover(2)");
        instructions.add("end(T1)");

        instructions.add("begin(T3)");
        instructions.add("R(T3, x6)");
        instructions.add("R(T3, x8)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] unfilteredLines = baos.toString().split("\n");
        String[] lines = filterLines(unfilteredLines);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        System.out.println(Arrays.toString(unfilteredLines));

        assertEquals("x3: 30", lines[0]);
        assertEquals("x5: 50", lines[1]);
        assertEquals("x6: 60", lines[2]);
        assertEquals("x8: 88", lines[3]);
    }

    // T1 ok. T2 ok. T2 reads from a recovering site, but odd variables only
    // at that site
    @Test
    void testUnreplicatedVariableIsReadSuccessfullyFromARecoveredSite() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("fail(3)");
        instructions.add("fail(4)");
        instructions.add("R(T1,x1)");
        instructions.add("W(T2,x8,88)");
        instructions.add("end(T1)");
        instructions.add("recover(4)");
        instructions.add("recover(3)");
        instructions.add("R(T2,x3)");
        instructions.add("end(T2)");

        instructions.add("begin(T3)");
        instructions.add("R(T3, x8)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x1: 10", lines[0]);
        assertEquals("x3: 30", lines[1]);
        assertEquals("x8: 88", lines[2]);
    }

    // T2 still reads the initial value of x3
    // T3 still reads the value of x3 written by T1
    @Test
    void testReadOnlyTransactionReadsOldValue() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("beginRO(T2)");
        instructions.add("R(T2,x1)");
        instructions.add("R(T2,x2)");
        instructions.add("W(T1,x3,33)");
        instructions.add("end(T1)");
        instructions.add("R(T2,x3)");
        instructions.add("end(T2)");

        instructions.add("beginRO(T3)");
        instructions.add("R(T3, x3)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x1: 10", lines[0]);
        assertEquals("x2: 20", lines[1]);
        assertEquals("x3: 30", lines[2]);
        assertEquals("x3: 33", lines[3]);
    }

    // T1, T2, T3 ok. Read from T3 waits for T2 to finish
    @Test
    void testWaitingOperationProperlyProcessed() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T3)");
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T3,x2,22)");
        instructions.add("W(T2,x4,44)");
        instructions.add("R(T3,x4)");
        instructions.add("end(T2)");
        instructions.add("end(T3)");
        instructions.add("R(T1,x2)");
        instructions.add("end(T1)");

//        instructions.add("beginRO(T3)");
//        instructions.add("R(T3, x3)");
//        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x4: 44", lines[0]);
        assertEquals("x2: 22", lines[1]);
//        assertEquals("30", lines[2]);
//        assertEquals("33", lines[3]);
    }

    @Test
    void testWaitingOperationsProcessedProperlyWhenLocksAreReleased() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("begin(T3)");
        instructions.add("W(T3,x2,22)");
        instructions.add("W(T2,x4,44)");
        instructions.add("R(T3,x4)");
        instructions.add("end(T2)");
        instructions.add("R(T1,x2)");
        instructions.add("end(T3)");
        instructions.add("end(T1)");

//        instructions.add("beginRO(T3)");
//        instructions.add("R(T3, x3)");
//        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x4: 44", lines[0]);
        assertEquals("x2: 22", lines[1]);
//        assertEquals("30", lines[2]);
//        assertEquals("33", lines[3]);
    }

    @Test
    void testWaitingWriteProcessedCorrectly() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x2)");
        instructions.add("R(T2,x2)");
        instructions.add("W(T2,x2,22)");
        instructions.add("end(T1)");
        instructions.add("end(T2)");

        instructions.add("begin(T3)");
        instructions.add("R(T3, x2)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x2: 20", lines[0]);
        assertEquals("x2: 20", lines[1]);
        assertEquals("x2: 22", lines[2]);
//        assertEquals("33", lines[3]);
    }


    @Test
    void testReadLockUpgradedToWriteSuccessfully() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("R(T1,x2)");
        instructions.add("R(T2,x2)");
        instructions.add("end(T1)");
        instructions.add("W(T2,x2,22)");
        instructions.add("end(T2)");

        instructions.add("begin(T3)");
        instructions.add("R(T3, x2)");
        instructions.add("end(T3)");
        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x2: 20", lines[0]);
        assertEquals("x2: 20", lines[1]);
        assertEquals("x2: 22", lines[2]);
//        assertEquals("33", lines[3]);
    }

    @Test
    void testDeadlockedTransactionAborted() throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("begin(T3)");
        instructions.add("W(T1, x1,5)");
        instructions.add("W(T3, x2,32)");
        instructions.add("W(T2, x1,17)");
        instructions.add("end(T1)");
        instructions.add("begin(T4)");
        instructions.add("W(T4, x4,35)");
        instructions.add("W(T3, x5,21)");
        instructions.add("W(T4,x2,21)");
        instructions.add("W(T3,x4,23)");
        instructions.add("end(T3)");
        instructions.add("end(T2)");
        instructions.add("dump()");

        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        System.out.println(Arrays.toString(lines));
//        assertEquals("20", lines[0]);
//        assertEquals("20", lines[1]);
//        assertEquals("22", lines[2]);
//        assertEquals("33", lines[3]);
    }

    @Test
    void test1 () throws Exception {
        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("W(T1, x1,5)");
        instructions.add("R(T2, x1)");
        instructions.add("R(T1, x1)");
        instructions.add("W(T1, x1, 6)");
        instructions.add("end(T1)");
        instructions.add("end(T2)");

        Driver.executeFromList(instructions);
        String[] lines = filterLines(baos.toString().split("\n"));
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        System.out.println(Arrays.toString(lines));
//        assertEquals("20", lines[0]);
//        assertEquals("20", lines[1]);
//        assertEquals("22", lines[2]);
//        assertEquals("33", lines[3]);
    }

    @Test
    void testDeadLockDetection () throws Exception {

        List<String> instructions = new ArrayList<>();
        instructions.add("begin(T1)");
        instructions.add("begin(T2)");
        instructions.add("begin(T3)");
        instructions.add("R(T1,x1)");
        instructions.add("W(T2,x1,50)");
        instructions.add("W(T3,x1,60)");
        instructions.add("W(T1,x1,70)");
        instructions.add("end(T1)");
        instructions.add("end(T2)");
        instructions.add("end(T3)");
        instructions.add("begin(T4)");
        instructions.add("R(T4,x1)");
        instructions.add("end(T4)");



        Driver.executeFromList(instructions);
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        String[] lines = filterLines(baos.toString().split("\n"));
//        System.out.println(Arrays.toString(lines));
        assertEquals("x1: 10", lines[0]);
        assertEquals("x1: 50", lines[1]);
//        assertEquals("22", lines[2]);
//        assertEquals("33", lines[3]);
    }


}

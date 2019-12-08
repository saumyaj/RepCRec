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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Test2 {

    private ByteArrayOutputStream baos;

    @BeforeEach
    void setUp() {
        baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
//        System.out.println(Arrays.toString(lines));
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
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
        String[] lines = baos.toString().split("\n");
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        System.out.println(Arrays.toString(lines));
//        assertEquals("20", lines[0]);
//        assertEquals("20", lines[1]);
//        assertEquals("22", lines[2]);
//        assertEquals("33", lines[3]);
    }

    @Test
    void test () throws Exception {
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
        String[] lines = baos.toString().split("\n");
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        System.out.println(Arrays.toString(lines));
//        assertEquals("20", lines[0]);
//        assertEquals("20", lines[1]);
//        assertEquals("22", lines[2]);
//        assertEquals("33", lines[3]);
    }

}

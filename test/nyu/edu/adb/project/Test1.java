package nyu.edu.adb.project;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Test1 {

    @Test
    void testingTest() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(baos));
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
//        String[] lines = baos.toString().split("\n");

        System.out.println(baos);
//        assertEquals("10", lines[0]);
//        assertEquals("-1", lines[0]);


    }
}

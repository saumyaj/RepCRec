package nyu.edu.adb.project;

class Variable {
    private final String name;
    private final int value;

    Variable(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public int getValue() {
        return value;
    }
}
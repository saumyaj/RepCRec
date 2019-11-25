package nyu.edu.adb.project;

class Transaction {
    private final String name;
    private final long beginTime;

    Transaction(String name, long tickTime) {
        this.name = name;
        beginTime = tickTime;
    }

    public String getName() {
        return name;
    }

    public long getBeginTime() {
        return beginTime;
    }
}
package nyu.edu.adb.project;

public class Database {
    long tickTime;
    int cycleDetectionInterval = 4;
    TransactionManager transactionManager;
    SiteManager siteManager;
    DataManager dataManager;
    private final int NUMBER_OF_SITES = 10;
    Database() {
        tickTime = 0;
        dataManager = new DataManager();
        siteManager = new SiteManager(NUMBER_OF_SITES);
        transactionManager = new TransactionManager(siteManager, dataManager);
    }
    public void handleQuery(String query) throws Exception {
        if (query==null) {
            throw new NullPointerException("query is null");
        }

        if (tickTime % cycleDetectionInterval==0) {
            //TODO - check for cycles in the waits-for graph
        }
        tickTime += 1;
        String paramsString = getParams(query);
        if (query.startsWith("beginRO(")) {
            beginReadOnlyTransaction(paramsString);
        } else if (query.startsWith("begin(")) {
            beginReadWriteTransaction(paramsString);
        } else if (query.startsWith("end")) {
            endTransaction(paramsString);
        } else if (query.startsWith("W(")) {
            write(paramsString);
        } else if (query.startsWith("R(")) {
            read(paramsString);
        }else if (query.equals("dump()")) {

        } else if (query.startsWith("fail(")) {

        } else if (query.startsWith("recover(")) {

        }
    }

    private String getParams(String query) {
        return query.substring(query.indexOf("(")+1,query.indexOf(")"));
    }

    private void beginReadWriteTransaction(String paramsString) throws Exception {
        transactionManager.createReadWriteTransaction(paramsString, tickTime);
    }

    private void beginReadOnlyTransaction(String paramsString) throws Exception {
        transactionManager.createReadOnlyTransaction(paramsString, tickTime);
    }

    private void endTransaction(String paramsString) {
        transactionManager.endTransaction(paramsString);
    }

    private void write(String paramsString) throws Exception {
        String[] params = paramsString.split(",");
        if (params.length == 3) {
            transactionManager.write(params[0].trim(), params[1].trim(), Integer.parseInt(params[2]));
        } else {
            throw new IllegalArgumentException("write operation must have 3 arguments");
        }
    }

    private void read(String paramsString) throws Exception {
        String[] params = paramsString.split(",");
        if (params.length == 2) {
            transactionManager.read( params[0].trim(), params[1].trim());
        } else {
            throw new IllegalArgumentException("write operation must have 3 arguments");
        }
    }
}
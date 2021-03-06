package nyu.edu.adb.project;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * This class is the public API of the main database system. The user of this database needs to call methods
 * of this class to interact with the database
 */
class Database {
    private long tickTime;
    private int cycleDetectionInterval = 1;
    TransactionManager transactionManager;
    private SiteManager siteManager;
    private WaitQueueManager waitQueueManager;
    private final int NUMBER_OF_SITES = 10;
    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    /**
     * @author Saumya
     */
    Database() {
        tickTime = 0;
        waitQueueManager = new WaitQueueManager();
        siteManager = new SiteManager(NUMBER_OF_SITES);
        transactionManager = new TransactionManager(siteManager, waitQueueManager);
        siteManager.setTransactionManager(transactionManager);
        initialize();
        LOGGER.setLevel(Level.WARNING);
    }

    /**
     * This is the parser method of the database. String type queries are parsed in this method and then
     * appropriate operation is performed by the database
     * @author Saumya
     */
    public void handleQuery(String query) throws Exception {
        if (query==null) {
            throw new NullPointerException("query is null");
        }

        if (tickTime % cycleDetectionInterval==0) {
            transactionManager.runDeadLockDetection();
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
            siteManager.dump();
        } else if (query.startsWith("fail(")) {
            failSite(paramsString);
        } else if (query.startsWith("recover(")) {
            recoverSite(paramsString);
        }
    }

    /**
     * @author Saumya
     */
    private String getParams(String query) {
        if(query.contains("(")) {
            return query.substring(query.indexOf("(")+1,query.indexOf(")"));
        }
        return null;
    }

    private void beginReadWriteTransaction(String paramsString) throws Exception {
        transactionManager.createReadWriteTransaction(paramsString, tickTime);
    }

    private void beginReadOnlyTransaction(String paramsString) throws Exception {
        transactionManager.createReadOnlyTransaction(paramsString, tickTime);
    }

    private void endTransaction(String paramsString) {
        transactionManager.endTransaction(paramsString, tickTime);
    }

    /**
     * @author Saumya
     */
    private void write(String paramsString) {
        String[] params = paramsString.split(",");
        if (params.length == 3) {
            transactionManager.write(params[0].trim(), params[1].trim(), Integer.parseInt(params[2].trim()));
        } else {
            throw new IllegalArgumentException("write operation must have 3 arguments");
        }
    }

    /**
     * @author Omkar
     */
    private void read(String paramsString) {

        String[] params = paramsString.split(",");
        if (params.length == 2) {
            String variableName = params[1].trim();
            Optional<Integer> readValue = transactionManager.read( params[0].trim(), variableName);
            if(readValue.isPresent()) {
                System.out.println(params[1].trim() + ": " + readValue.get());
            } else {
                LOGGER.log(Level.INFO, "read failed for transaction " + params[0].trim());
            }
        } else {
            throw new IllegalArgumentException("write operation must have 3 arguments");
        }
    }

    /**
     * @author Omkar
     */
    private void recoverSite(String paramsString) {
        String[] params = paramsString.split(",");
        if(params.length == 1) {
            int siteId = Integer.parseInt(params[0].trim());
            siteManager.recoverSite(siteId);
        } else {
            throw new IllegalArgumentException("recover operation must have one argument");
        }
    }

    /**
     * @author Omkar
     */
    private void failSite(String paramsString) {
        String[] params = paramsString.split(",");
        if(params.length == 1) {
            int siteId = Integer.parseInt(params[0].trim());
            siteManager.failSite(siteId);
            transactionManager.checkTransactionsForAbortionAfterSiteFailure(siteId);
        } else {
            throw new IllegalArgumentException("fail operation must have one argument");
        }
    }

    /**
     * Helper method to initialize the database for this project
     * @author Omkar
     */
    void initialize() {
        siteManager.initializeVariables();
    }
}

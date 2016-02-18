import breeze.optimize.linear.LinearProgram;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;


class PmTable {
    String dbTableName;
    boolean isMutable;
    DataFrame dataFrame;
    String cachePath;
    String partitionKey;
    boolean isIncrement;

    PmTable(String dbTableName, boolean isMutable, String cachePath, String partitionKey, boolean isIncrement) {
        this.cachePath = cachePath;
        this.dbTableName = dbTableName;
        this.isMutable = isMutable;
        this.partitionKey = partitionKey;
        this.isIncrement = isIncrement;
    }

}


public class DataProvider {

    private Map<String, String[]> preparedDataFolders = new HashMap<String, String[]>();
    private AtomicBoolean isDataReady = new AtomicBoolean(false);
    private final CopyOnWriteArrayList<PmTable> tablesList = new CopyOnWriteArrayList<PmTable>();
    private static String ORACLE_CONNECTION_URL;

    private ArrayList<String> generations = new ArrayList<String>();

    //String debugLimitation = "";
    String debugLimitation = " AND ROWNUM < 1000";
    //SparkConf conf;
    JavaSparkContext sparkContext;
    Properties connectionProperties;

    DataProvider(JavaSparkContext sparkContext){

        this.sparkContext = sparkContext;

        PropertiesConfiguration config = new PropertiesConfiguration();
        try {
            config.load("config.properties");
        } catch (ConfigurationException ex) {
            System.out.println("WARNINIG: Configuration parameters could not be read. Program quits");
            System.exit(0);
        }

        /*
        ORACLE_CONNECTION_URL = "jdbc:oracle:thin:" + config.getString("ORACLE_USERNAME") + "/" + config.getString("ORACLE_PWD") +
                "@//" + config.getString("ORACLE_HOST") + ":" + config.getString("ORACLE_PORT") + "/" + config.getString("ORACLE_SERVICE");
        */

        ORACLE_CONNECTION_URL = config.getString("ORACLE_CONNECTION_URL");
        connectionProperties = new Properties();
        connectionProperties.setProperty("driver", config.getString("ORACLE_DRIVER"));
        connectionProperties.setProperty("user", config.getString("ORACLE_USERNAME"));
        connectionProperties.setProperty("password", config.getString("ORACLE_PWD"));
        //this.conf = conf;
    }


    synchronized Map<String, String[]> getDataFolders() {
        if (isDataReady.get())
            return preparedDataFolders;
        else
            return null;
    }

    public void registerTable(PmTable table) {
        tablesList.add(table);
    }

    public void updateTablesAsynchro(){
        Thread t = new Thread(new Runnable() {
            public void run() {
                updateTables();
            }
        });
        t.start();
    }

    public void updateTables() {

        final BigDecimal previousUpdateTime = new BigDecimal(0);
        //JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("ERROR");
        sparkContext.setLocalProperty("spark.driver.allowMultipleContexts","true");


        while (true) {

            System.out.println("Update Started:" + new Date());

            final String previousCacheSuffix;
            if (generations.size() == 0)
                previousCacheSuffix = null;
            else
                previousCacheSuffix = generations.get(generations.size()-1);

            final SQLContext sqlContext = new SQLContext(sparkContext);
            ArrayList<Thread> threads = new ArrayList<Thread>();
            final String currentCacheSuffix = RandomStringUtils.randomAlphabetic(10);
            final DataPreprocessor dataPreprocessor = new DataPreprocessor();

            for (final PmTable table : tablesList) {
                Thread t = new Thread(new Runnable() {
                    public void run() {

                        if (table.isMutable ) {

                            String tableSQL = "(SELECT " + table.dbTableName + ".* FROM " +
                                    table.dbTableName + " WHERE ";
                            tableSQL += (table.isIncrement)? (" DBMODTIME >= " + previousUpdateTime): "1=1";
                            tableSQL +=  " " +
                                    debugLimitation + ") tt"; //DBMODTIME<" + endTime + " AND ///ORA_ROWSCN as DBMODTIME,

                            DataFrame newDF;
                            if (table.isIncrement)
                                newDF = sqlContext.read().jdbc(ORACLE_CONNECTION_URL, tableSQL, "PARTID", 0, 20, 4, connectionProperties);
                            else
                                newDF = sqlContext.read().jdbc(ORACLE_CONNECTION_URL, tableSQL, connectionProperties);

                            DataFrame newDF1 = dataPreprocessor.preprocess(newDF, table.dbTableName);

                            newDF1.write().parquet(table.cachePath + "." + currentCacheSuffix + ".tmp");
                            newDF1 = sqlContext.read().parquet(table.cachePath + "." + currentCacheSuffix + ".tmp");
                            newDF1.registerTempTable("newDF");

                            if (previousCacheSuffix != null && table.isIncrement) {
                                DataFrame oldDF = sqlContext.read().parquet(table.cachePath + "." + previousCacheSuffix);
                                oldDF.registerTempTable("oldDF");
                                table.dataFrame = newDF.unionAll(sqlContext.sql("SELECT oldDF.* FROM oldDF LEFT OUTER JOIN newDF ON oldDF.PANDAID=newDF.PANDAID WHERE newDF.PANDAID IS NULL"));
                            } else {
                                table.dataFrame = newDF;
                            }
                            table.dataFrame.saveAsParquetFile(table.cachePath + "." + currentCacheSuffix);
                            System.gc();
                            generations.add(currentCacheSuffix);
                            synchronized (preparedDataFolders) {
                                preparedDataFolders.put(table.dbTableName, new String[]{table.cachePath + "." + currentCacheSuffix});
                            }
                        }
                    }
                });
                t.start();
                threads.add(t);
            }

            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException ex) {
                    ;
                }
            }
            isDataReady.set(true);
            System.out.println("Update Finished:" + new Date());
        }
    }


}

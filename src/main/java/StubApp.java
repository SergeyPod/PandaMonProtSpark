import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by podolsky on 12.02.16.
 */


public class StubApp {

    private static Gson gson = new Gson();

    public static void main(String[] args) throws Exception {
        PmTable jobsTable = new PmTable("ATLAS_PANDABIGMON.COMBINED_WAIT_ACT_DEF_ARCH4", true, "/fasttmp/spark/COMBINED_WAIT_ACT_DEF_ARCH4", "PANDAID", true);
        PmTable cloudConfig = new PmTable("ATLAS_PANDAMETA.CLOUDCONFIG", true, "/fasttmp/spark/CLOUDCONFIG", "", false);
        PmTable schedConfig = new PmTable("ATLAS_PANDAMETA.SCHEDCONFIG", true, "/fasttmp/spark/SCHEDCONFIG", "", false);

        SparkConf conf = new SparkConf().setAppName("serverApp").setMaster("local[2]").set("spark.local.dir","/fasttmp/spark").
                set("spark.driver.allowMultipleContexts","true").set("spark.ui.enabled", "false").
                set("spark.streaming.unpersist","true");//set("spark.cleaner.ttl", "5200");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);


        DataProvider dataProvider = new DataProvider(sparkContext);
        dataProvider.registerTable(jobsTable);
        dataProvider.registerTable(cloudConfig);
        dataProvider.registerTable(schedConfig);
        dataProvider.updateTablesAsynchro();

        DataProcessor dataProcessor = new DataProcessor(dataProvider, sparkContext);

        ErrorSummaryJSONRequest es = new ErrorSummaryJSONRequest();
        es.JOBType = "";

        while (dataProvider.getDataFolders() == null)
            Thread.sleep(1000*10);
        dataProcessor.errorSummary(es);

        /*
        for (int i = 0; i < 10; i++) {
            System.out.println(gson.toJson(dataProvider.getDataFolders()));
            Thread.sleep(1000*60*10);
        }
        */
    }
}
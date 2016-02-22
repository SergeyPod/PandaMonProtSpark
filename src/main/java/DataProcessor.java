import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.util.SystemClock;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by podolsky on 13.02.16.
 */
public class DataProcessor {

    private DataProvider dataProvider;
    private JavaSparkContext sparkContext;
    private ArrayList<Map<String,String>> errorCodeListOfMaps;
    private final String debFolder = "QIOzmqXQFt";

    DataProcessor(DataProvider dataProvider, JavaSparkContext sparkContext) {
        this.dataProvider = dataProvider;
        this.sparkContext = sparkContext;
    }

    String errorSummary(ErrorSummaryJSONRequest query){


        //String[] tableQueryParameters = {"computingsite", "jobstatus", "produsername", "specialhandling","produsername", "jeditaskid", "taskid"};

        //JavaSparkContext sparkContext = new JavaSparkContext(conf);

        //sparkContext.setLogLevel("ERROR");

        sparkContext.setLocalProperty("spark.driver.allowMultipleContexts","true");
        sparkContext.setLocalProperty("spark.scheduler.allocation.file", "transfer");


        final SQLContext sqlContext = new SQLContext(sparkContext);

        //String [] dataFolders = dataProvider.getDataFolders().get("ATLAS_PANDABIGMON.COMBINED_WAIT_ACT_DEF_ARCH4");
        String [] dataFolders = new String[]{"/fasttmp/spark/COMBINED_WAIT_ACT_DEF_ARCH4."+debFolder};

        if (dataFolders == null || dataFolders.length == 0)
            return "Error with Data Source";

        System.out.println("Mapping Started: " + (new Date()));

        DataFrame sourceDataFrame = sqlContext.read().parquet(dataFolders[0]);
        for (int i = 1; i < dataFolders.length; i++)
            sourceDataFrame = sourceDataFrame.unionAll(sqlContext.read().parquet(dataFolders[0]));


        final Accumulator<Map<Date, ErrorSummaryerrsByTime>> errorsSummaryAccByTime = sparkContext.accumulator( new HashMap<Date,
                ErrorSummaryerrsByTime>(), "errsByTime", new ErrorsSummaryProcessorAccParamDate<ErrorSummaryerrsByTime>());
        final Accumulator<Map<String, ErrorSummaryerrsByCount>> errorsSummaryAccByCount = sparkContext.accumulator( new HashMap<String,
                ErrorSummaryerrsByCount>(), "errsByCount", new ErrorsSummaryProcessorAccParam<ErrorSummaryerrsByCount>());
        final Accumulator<Map<String, ErrorSummaryerrsBySite>> errorsSummaryAccBySite = sparkContext.accumulator(new HashMap<String,
                ErrorSummaryerrsBySite>(), "errsBySite", new ErrorsSummaryProcessorAccParam<ErrorSummaryerrsBySite>());
        final Accumulator<Map<String, ErrorSummaryerrsByTask>> errorsSummaryAccByTask = sparkContext.accumulator(new HashMap<String,
                ErrorSummaryerrsByTask>(), "errsByTask", new ErrorsSummaryProcessorAccParam<ErrorSummaryerrsByTask>());
        final Accumulator<Map<String, ErrorSummaryerrsByUser>> errorsSummaryAccByUser = sparkContext.accumulator(new HashMap<String,
                ErrorSummaryerrsByUser>(), "errsByUser", new ErrorsSummaryProcessorAccParam<ErrorSummaryerrsByUser>());



        final Accumulator<Map<String, Integer>> errorsSummaryAccBySiteJobs = sparkContext.accumulator(new HashMap<String, Integer>(), "errorsSummaryAccBySiteJobs", new MapIntegerAccumulator());
        final Accumulator<Map<BigDecimal, Integer>> errorSummaryerrsByTaskJobs = sparkContext.accumulator(new HashMap<BigDecimal, Integer>(), "ErrorSummaryerrsByTaskJobs", new MapIntegerAccumulator());

        ErrorsSummaryProcessorMap map = new ErrorsSummaryProcessorMap(ErrorSummaryJSONRequest query);
        map.seterrorSummaryerrsByTime(errorsSummaryAccByTime);
        map.setErrorsSummaryAccByCount(errorsSummaryAccByCount);
        map.setErrorsSummaryAccBySite(errorsSummaryAccBySite);
        map.setErrorsSummaryAccByTask(errorsSummaryAccByTask);
        map.setErrorsSummaryAccByUser(errorsSummaryAccByUser);
        map.setErrorsSummaryAccBySiteJobs(errorsSummaryAccBySiteJobs);
        map.seterrorSummaryerrsByTaskJobs(errorSummaryerrsByTaskJobs);

        sourceDataFrame.filter( " (JOBSTATUS IN ('failed', 'holding')) AND (MODIFICATIONTIME > (now() - INTERVAL 15 HOUR)) " ) .toJavaRDD().foreach(map);

        /*

        condition = \
  (to_date(df.start_time) == to_date(df.end_time)) & \
  (df.start_time + expr("INTERVAL 1 HOUR") >= df.end_time)


         */


/*
        Map<BigDecimal, Integer> example = errorSummaryerrsByTaskJobs.value();
        for (BigDecimal name: example.keySet()){
            String key =name.toString();
            String value = example.get(name).toString();
            System.out.println(key + " " + value);
        }
        System.out.println("Mapping finished:" + (new Date()));
*/
        Map<BigDecimal, Integer> example = errorSummaryerrsByTaskJobs.value();
        for (BigDecimal name: example.keySet()){
            String key =name.toString();
            String value = example.get(name).toString();
            System.out.println(key + " " + value);
        }
        System.out.println("Mapping finished:" + (new Date()));


        Map<Date, ErrorSummaryerrsByTime> example1= errorsSummaryAccByTime.value();
        for (Date name: example1.keySet()){
            String key =name.toString();
            long value = ((ErrorSummaryerrsByTime) example1.get(name)).count;
            System.out.println(key + " " + value);
        }
        System.out.println("Mapping finished:" + (new Date()));

        return null;
    }
}

import com.google.gson.Gson;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.sql.Row;
import org.apache.spark.util.SystemClock;
import scala.collection.JavaConversions;
import scala.collection.TraversableOnce;
import com.google.gson.reflect.TypeToken;
import scala.math.Ordering;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by podolsky on 17.02.16.
 */
public class ErrorsSummaryProcessorMap implements VoidFunction<Row> {

    int count = 0;

    ArrayList<Map<String,String>> errorCodeListOfMaps;
    private Accumulator<Map<String, ErrorSummaryerrsByCount>> errorsSummaryAccByCount;
    private Accumulator<Map<String, ErrorSummaryerrsBySite>> errorsSummaryAccBySite;
    private Accumulator<Map<String, ErrorSummaryerrsByTask>> errorsSummaryAccByTask;
    private Accumulator<Map<String, ErrorSummaryerrsByUser>> errorsSummaryAccByUser;
    private Accumulator<Map<String, Integer>> errorsSummaryAccBySiteJobs;
    private Accumulator<Map<BigDecimal, Integer>> errorSummaryerrsByTaskJobs;

    public void setErrorsSummaryAccByCount(Accumulator<Map<String, ErrorSummaryerrsByCount>> errorsSummaryAccByCount){
        this.errorsSummaryAccByCount = errorsSummaryAccByCount;
    }

    public void setErrorsSummaryAccBySite(Accumulator<Map<String, ErrorSummaryerrsBySite>> errorsSummaryAccBySite){
        this.errorsSummaryAccBySite = errorsSummaryAccBySite;
    }

    public void setErrorsSummaryAccByTask(Accumulator<Map<String, ErrorSummaryerrsByTask>> errorsSummaryAccByTask){
        this.errorsSummaryAccByTask = errorsSummaryAccByTask;
    }

    public void setErrorsSummaryAccByUser(Accumulator<Map<String, ErrorSummaryerrsByUser>> errorsSummaryAccByUser){
        this.errorsSummaryAccByUser = errorsSummaryAccByUser;
    }

    public void setErrorsSummaryAccBySiteJobs(Accumulator<Map<String, Integer>> errorsSummaryAccBySiteJobs){
        this.errorsSummaryAccBySiteJobs = errorsSummaryAccBySiteJobs;
    }

    public void seterrorSummaryerrsByTaskJobs(Accumulator<Map<BigDecimal, Integer>> errorSummaryerrsByTaskJobs){
        this.errorSummaryerrsByTaskJobs = errorSummaryerrsByTaskJobs;
    }




    ErrorsSummaryProcessorMap (Accumulator<Map<String, ? extends Object>>... accumulators){

        Gson gson = new Gson();
        Type mapOfStringObjectType = new TypeToken< ArrayList<Map<String,String>> >() {}.getType();
        errorCodeListOfMaps = gson.fromJson(CONSTS.errorcodelist, mapOfStringObjectType);


    }


    public static boolean checkIsField(Row row, String fieldName) {
            return Arrays.asList(row.schema().fieldNames()).contains(fieldName.toUpperCase());
    }

    public void call(Row row) {
            Iterator errorCodesIterator = errorCodeListOfMaps.iterator();
            while (errorCodesIterator.hasNext()) {
                Map<String, String> err = (Map<String, String>) errorCodesIterator.next();


                if ((err != null) && checkIsField(row, err.get("diag")))
                if ((row.getAs(err.get("error").toUpperCase()) != null) && (((String) row.getAs(err.get("error").toUpperCase()).toString()).length() > 0)) {
                    String errval = row.getAs(err.get("error").toUpperCase()).toString();
                    String errdiag = "";
                    String errnum;

                    try {
                        int errnumInt = Integer.getInteger(errval);
                    /*
                       if err['error'] in errorCodes and errnum in errorCodes[err['error']]:
                        errdiag = errorCodes[err['error']][errnum]

                     */
                        errnum = new Integer(errnumInt).toString();
                    } catch (Exception e) {
                        errnum = errval;
                    }
                    String errcode = String.format("%s:%s", err.get("name"), errnum);



                    if (err.get("diag") != null)
                        errdiag = (String) row.getAs(err.get("diag").toUpperCase());

                    ErrorSummaryerrsByCount errsByCount = new ErrorSummaryerrsByCount();
                    errsByCount.error = errcode;
                    errsByCount.codename = err.get("error");
                    errsByCount.codeval = errnum;
                    errsByCount.diag = errdiag;
                    errsByCount.count = 1;

                    ErrorSummaryerrsByUser errsByUser = new ErrorSummaryerrsByUser();
                    errsByUser.errors = new TreeMap<String, ErrorSummaryerrsByCount>();
                    errsByUser.errors.put(errcode, errsByCount);
                    errsByUser.name = row.getAs("PRODUSERNAME");
                    errsByUser.toterrors = 1;

                    ErrorSummaryerrsBySite errsBySite = new ErrorSummaryerrsBySite();
                    errsBySite.name = row.getAs("COMPUTINGSITE");
                    errsBySite.errors = new TreeMap<String, ErrorSummaryerrsByCount>();
                    errsBySite.errors.put(errcode, errsByCount);
                    errsBySite.toterrors = 1;


                    ErrorSummaryerrsByTask errsByTask = new ErrorSummaryerrsByTask();
                    errsByTask.errcode = errcode;
                    errsByTask.longname = row.getAs("TASKNAME");
                    errsByTask.name = row.getAs("JEDITASKID");
                    errsByTask.tasktype = "jeditaskid";
                    errsByTask.toterrors = 1;
                    errsByTask.errors = new TreeMap<String, ErrorSummaryerrsByCount>();

                    Map<String, ErrorSummaryerrsByCount> map1 = new TreeMap<String, ErrorSummaryerrsByCount>();
                    map1.put(errcode, errsByCount);
                    errorsSummaryAccByCount.add(map1);

                    Map<String, ErrorSummaryerrsByUser> map2 = new TreeMap<String, ErrorSummaryerrsByUser>();
                    map2.put(errcode, errsByUser);
                    errorsSummaryAccByUser.add(map2);

                    Map<String, ErrorSummaryerrsBySite> map3 = new TreeMap<String, ErrorSummaryerrsBySite>();
                    map3.put(errcode, errsBySite);
                    errorsSummaryAccBySite.add(map3);

                    Map<String, ErrorSummaryerrsByTask> map4 = new TreeMap<String, ErrorSummaryerrsByTask>();
                    map4.put(errcode, errsByTask);
                    errorsSummaryAccByTask.add(map4);
                }
            }

            Map<String, Integer> map5 =  new TreeMap<>();
            map5.put(row.getAs("COMPUTINGSITE"), 1);
            errorsSummaryAccBySiteJobs.add(map5);
            Map<BigDecimal, Integer> map6 =  new TreeMap<>();
            map6.put(  (row.getAs("JEDITASKID") != null)?row.getAs("JEDITASKID"): (new BigDecimal(0)) , 1);
            errorSummaryerrsByTaskJobs.add(map6);
        }
    }


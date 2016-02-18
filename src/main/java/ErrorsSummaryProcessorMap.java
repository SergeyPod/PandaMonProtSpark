import com.google.gson.Gson;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.sql.Row;
import scala.collection.JavaConversions;
import scala.collection.TraversableOnce;
import com.google.gson.reflect.TypeToken;
import scala.math.Ordering;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by podolsky on 17.02.16.
 */
public class ErrorsSummaryProcessorMap<T> implements Function<Row, Row> {

    int count = 0;

    ArrayList<Map<String,String>> errorCodeListOfMaps;

    Map<String, Accumulator<ErrorsSummaryProcessorAccumulator<T>>> accumulators;

    ErrorsSummaryProcessorMap (Accumulator<ErrorsSummaryProcessorAccumulator<T>>... accumulators){
        this.accumulators = new HashMap<String, Accumulator<ErrorsSummaryProcessorAccumulator<T>>>(accumulators.length);

        for (Accumulator<ErrorsSummaryProcessorAccumulator<T>> accumulator: accumulators) {
            this.accumulators.put(accumulator.name().get(), accumulator);
        }


        Gson gson = new Gson();
        Type mapOfStringObjectType = new TypeToken< ArrayList<Map<String,String>> >() {}.getType();
        errorCodeListOfMaps = gson.fromJson(CONSTS.errorcodelist, mapOfStringObjectType);

    }

    public Row call(Row row) {

        Iterator errorCodesIterator = errorCodeListOfMaps.iterator();

        while (errorCodesIterator.hasNext()) {
            Map<String,String> err = (Map<String,String>) errorCodesIterator.next();
            if ((row.getAs( err.get("error") ) != null) && ( ((String)row.getAs( err.get("error"))).length() > 0)) {
                String errval = row.getAs( err.get("error"));
                String errdiag = "";
                String errnum;

                try {
                    int errnumInt = Integer.getInteger(errval);
                    /*
                       if err['error'] in errorCodes and errnum in errorCodes[err['error']]:
                        errdiag = errorCodes[err['error']][errnum]

                     */
                    errnum = new Integer(errnumInt).toString();
                }
                catch (Exception e) {
                    errnum = errval;
                }
                String errcode = String.format("%s:%s" , err.get("name"), errnum );
                if (err.get("diag") != null)
                    errdiag = (String)row.getAs( err.get("diag"));

                ErrorSummaryerrsByCount errsByCount = new ErrorSummaryerrsByCount();
                errsByCount.error = errcode;
                errsByCount.codename = err.get("error");
                errsByCount.codeval = errnum;
                errsByCount.diag = errdiag;
                errsByCount.count = new AtomicInteger(1);

                ErrorSummaryerrsByUser errsByUser = new ErrorSummaryerrsByUser();
                errsByUser.errors = new HashMap<String, ErrorSummaryerrsByCount>();
                errsByUser.errors.put( errcode, errsByCount);
                errsByUser.name = row.getAs("produsername");
                errsByUser.toterrors = new AtomicInteger(1);


                ErrorSummaryerrsBySite errsBySite = new ErrorSummaryerrsBySite();
                errsBySite.name = row.getAs("computingsite");
                errsBySite.errors = new HashMap<String, ErrorSummaryerrsByCount>();
                errsBySite.errors.put( errcode, errsByCount);
                errsBySite.toterrors = new AtomicInteger(1);


                ErrorSummaryerrsByTask errsByTask = new ErrorSummaryerrsByTask();
                errsByTask.errcode = errcode;
                errsByTask.longname = row.getAs("taskname");
                errsByTask.name = row.getAs("produsername");
                errsByTask.tasktype = "jeditaskid";
                errsByTask.toterrors = new AtomicInteger(1);

                ErrorsSummaryProcessorAccumulator rr1= new ErrorsSummaryProcessorAccumulator<ErrorSummaryerrsByCount>(errsByCount);
                ErrorsSummaryProcessorAccumulator rr2= new ErrorsSummaryProcessorAccumulator<ErrorSummaryerrsBySite>(errsBySite);
                ErrorsSummaryProcessorAccumulator rr3= new ErrorsSummaryProcessorAccumulator<ErrorSummaryerrsByTask>(errsByTask);
                ErrorsSummaryProcessorAccumulator rr4= new ErrorsSummaryProcessorAccumulator<ErrorSummaryerrsByUser>(errsByUser);

                accumulators.get("errsByCount").add(rr1);
                accumulators.get("errsBySite").add(rr2);
                accumulators.get("errsByTask").add(rr3);
                accumulators.get("errsByUser").add(rr4);
            }
        }





        return null;
    }


    /*


                if errcode not in errsByCount:
                    errsByCount[errcode] = {}
                    errsByCount[errcode]['error'] = errcode
                    errsByCount[errcode]['codename'] = err['error']
                    errsByCount[errcode]['codeval'] = errnum
                    errsByCount[errcode]['diag'] = errdiag
                    errsByCount[errcode]['count'] = 0
                errsByCount[errcode]['count'] += 1

                if user not in errsByUser:
                    errsByUser[user] = {}
                    errsByUser[user]['name'] = user
                    errsByUser[user]['errors'] = {}
                    errsByUser[user]['toterrors'] = 0
                if errcode not in errsByUser[user]['errors']:
                    errsByUser[user]['errors'][errcode] = {}
                    errsByUser[user]['errors'][errcode]['error'] = errcode
                    errsByUser[user]['errors'][errcode]['codename'] = err['error']
                    errsByUser[user]['errors'][errcode]['codeval'] = errnum
                    errsByUser[user]['errors'][errcode]['diag'] = errdiag
                    errsByUser[user]['errors'][errcode]['count'] = 0
                errsByUser[user]['errors'][errcode]['count'] += 1
                errsByUser[user]['toterrors'] += 1

                if site not in errsBySite:
                    errsBySite[site] = {}
                    errsBySite[site]['name'] = site
                    errsBySite[site]['errors'] = {}
                    errsBySite[site]['toterrors'] = 0
                    errsBySite[site]['toterrjobs'] = 0
                if errcode not in errsBySite[site]['errors']:
                    errsBySite[site]['errors'][errcode] = {}
                    errsBySite[site]['errors'][errcode]['error'] = errcode
                    errsBySite[site]['errors'][errcode]['codename'] = err['error']
                    errsBySite[site]['errors'][errcode]['codeval'] = errnum
                    errsBySite[site]['errors'][errcode]['diag'] = errdiag
                    errsBySite[site]['errors'][errcode]['count'] = 0
                errsBySite[site]['errors'][errcode]['count'] += 1
                errsBySite[site]['toterrors'] += 1

                if tasktype == 'jeditaskid' or taskid > 1000000 or 'produsername' in request.session['requestParams']:
                    if taskid not in errsByTask:
                        errsByTask[taskid] = {}
                        errsByTask[taskid]['name'] = taskid
                        errsByTask[taskid]['longname'] = taskname
                        errsByTask[taskid]['errors'] = {}
                        errsByTask[taskid]['toterrors'] = 0
                        errsByTask[taskid]['toterrjobs'] = 0
                        errsByTask[taskid]['tasktype'] = tasktype
                    if errcode not in errsByTask[taskid]['errors']:
                        errsByTask[taskid]['errors'][errcode] = {}
                        errsByTask[taskid]['errors'][errcode]['error'] = errcode
                        errsByTask[taskid]['errors'][errcode]['codename'] = err['error']
                        errsByTask[taskid]['errors'][errcode]['codeval'] = errnum
                        errsByTask[taskid]['errors'][errcode]['diag'] = errdiag
                        errsByTask[taskid]['errors'][errcode]['count'] = 0
                    errsByTask[taskid]['errors'][errcode]['count'] += 1
                    errsByTask[taskid]['toterrors'] += 1
        if site in errsBySite: errsBySite[site]['toterrjobs'] += 1
        if taskid in errsByTask: errsByTask[taskid]['toterrjobs'] += 1




     */





/*
    public TraversableOnce<IndexedRow> apply(IndexedRow in){
        count++;
        List<IndexedRow> tokens = new ArrayList<IndexedRow>();
        ErrorsSummaryProcessorAccumulator rr= new ErrorsSummaryProcessorAccumulator(in);
        accumulator.add(rr);
        return JavaConversions.collectionAsScalaIterable(tokens);
    }
 */
}

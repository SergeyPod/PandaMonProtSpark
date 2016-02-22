import org.apache.spark.AccumulatorParam;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by spadolski on 2/18/16.
 */



public class ErrorsSummaryProcessorAccParam<T> implements AccumulatorParam<Map<String, T>>, Serializable {
    // only use PARAM_INSTANCE
    public ErrorsSummaryProcessorAccParam() {}
    public Map<String, T> addAccumulator(final Map<String, T> r, final Map<String, T> t) { return mergeMap(r,t); }
    public Map<String, T> addInPlace(final Map<String, T> r, final Map<String, T> t) {
        return mergeMap(r,t);
    }
    public Map<String, T> zero(final Map<String, T> initialValue) {
        return new HashMap<>();
    }
    private Map<String, T> mergeMap( Map<String, T> map1, Map<String, T> map2) {
        Map<String, T> result = new TreeMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (obj1, obj2) ->
        {
            if (obj1 instanceof ErrorSummaryerrsByCount) {
                ((ErrorSummaryerrsByCount)obj1).count +=  ((ErrorSummaryerrsByCount)obj2).count;
            }
            if (obj1 instanceof ErrorSummaryerrsByUser) {
                ((ErrorSummaryerrsByUser)obj1).mergeErrors(((ErrorSummaryerrsByUser)obj2).errors);
                ((ErrorSummaryerrsByUser)obj1).toterrors +=  ((ErrorSummaryerrsByUser)obj2).toterrors;
            }
            if (obj1 instanceof ErrorSummaryerrsBySite) {
                ((ErrorSummaryerrsBySite)obj1).mergeErrors(((ErrorSummaryerrsBySite)obj2).errors);
                ((ErrorSummaryerrsBySite)obj1).toterrors +=  ((ErrorSummaryerrsBySite)obj2).toterrors;
            }
            if (obj1 instanceof ErrorSummaryerrsByTask) {
                ((ErrorSummaryerrsByTask)obj1).mergeErrors(((ErrorSummaryerrsByTask)obj2).errors);
                ((ErrorSummaryerrsByTask)obj1).toterrors += ((ErrorSummaryerrsByTask)obj2).toterrors;
            }

            return obj1;
        }));

        return result;
    }
}




/*


 */




class MapIntegerAccumulator<T> implements AccumulatorParam<Map<T, Integer>>, Serializable {

    @Override
    public Map<T, Integer> addAccumulator(Map<T, Integer> t1, Map<T, Integer> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<T, Integer> addInPlace(Map<T, Integer> r1, Map<T, Integer> r2) {
        return mergeMap(r1, r2);

    }

    @Override
    public Map<T, Integer> zero(final Map<T, Integer> initialValue) {
        return new HashMap<>();
    }

    private Map<T, Integer> mergeMap( Map<T, Integer> map1, Map<T,Integer> map2) {
        Map<T, Integer> result = new TreeMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (a, b) -> a + b));
        return result;
    }
}






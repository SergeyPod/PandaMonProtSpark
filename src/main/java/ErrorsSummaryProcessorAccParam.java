import org.apache.spark.AccumulatorParam;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
        Map<String, T> result = new HashMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (obj1, obj2) ->
        {
            if (obj1 instanceof ErrorSummaryerrsByCount) {
                ((ErrorSummaryerrsByCount)obj1).count.addAndGet( ((ErrorSummaryerrsByCount)obj2).count.get());
            }
            if (obj1 instanceof ErrorSummaryerrsByUser) {
                ((ErrorSummaryerrsByUser)obj1).mergeErrors(((ErrorSummaryerrsByUser)obj2).errors);
                ((ErrorSummaryerrsByUser)obj1).toterrors.addAndGet( ((ErrorSummaryerrsByUser)obj2).toterrors.get());
            }
            if (obj1 instanceof ErrorSummaryerrsBySite) {
                ((ErrorSummaryerrsBySite)obj1).mergeErrors(((ErrorSummaryerrsBySite)obj2).errors);
                ((ErrorSummaryerrsBySite)obj1).toterrors.addAndGet( ((ErrorSummaryerrsBySite)obj2).toterrors.get());
            }
            if (obj1 instanceof ErrorSummaryerrsByTask) {
                ((ErrorSummaryerrsByTask)obj1).mergeErrors(((ErrorSummaryerrsByTask)obj2).errors);
                ((ErrorSummaryerrsByTask)obj1).toterrors.addAndGet( ((ErrorSummaryerrsByTask)obj2).toterrors.get());
            }

            return obj1;
        }));

        return result;
    }
}


class MapIntegerAccumulator implements AccumulatorParam<Map<String, Integer>>, Serializable {

    @Override
    public Map<String, Integer> addAccumulator(Map<String, Integer> t1, Map<String, Integer> t2) {
        return mergeMap(t1, t2);
    }

    @Override
    public Map<String, Integer> addInPlace(Map<String, Integer> r1, Map<String, Integer> r2) {
        return mergeMap(r1, r2);

    }

    @Override
    public Map<String, Integer> zero(final Map<String, Integer> initialValue) {
        return new HashMap<>();
    }

    private Map<String, Integer> mergeMap( Map<String, Integer> map1, Map<String, Integer> map2) {
        Map<String, Integer> result = new HashMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (a, b) -> a + b));
        return result;
    }
}






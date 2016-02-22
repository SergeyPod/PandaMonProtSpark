import org.apache.spark.AccumulatorParam;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by spadolski on 2/22/16.
 */
public class ErrorsSummaryProcessorAccParamDate<T> implements AccumulatorParam<Map<Date, T>>, Serializable {
    // only use PARAM_INSTANCE
    public ErrorsSummaryProcessorAccParamDate() {}

    public Map<Date, T> addAccumulator(final Map<Date, T> r, final Map<Date, T> t) { return mergeMap(r,t); }
    public Map<Date, T> addInPlace(final Map<Date, T> r, final Map<Date, T> t) {
        return mergeMap(r,t);
    }
    public Map<Date, T> zero(final Map<Date, T> initialValue) {
        return new HashMap<>();
    }

    private Map<Date, T> mergeMap( Map<Date, T> map1, Map<Date, T> map2) {
        Map<Date, T> result = new TreeMap<>(map1);
        map2.forEach((k, v) -> result.merge(k, v, (obj1, obj2) ->
        {
            if (obj1 instanceof ErrorSummaryerrsByTime) {
                ((ErrorSummaryerrsByTime)obj1).count +=  ((ErrorSummaryerrsByTime)obj2).count;
            }
            return obj1;
        }));

        return result;
    }

}

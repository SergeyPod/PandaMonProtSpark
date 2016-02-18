import org.apache.spark.AccumulatorParam;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class ErrorsSummaryProcessorAccumulator<T> implements Serializable {

    public static final MaxIndexRowsAccumulatorParam PARAM_INSTANCE = new MaxIndexRowsAccumulatorParam();
    public static final ErrorsSummaryProcessorAccumulator ZERO = new ErrorsSummaryProcessorAccumulator();

    Map<String, T> internalMap;

    private ErrorsSummaryProcessorAccumulator(){
        internalMap = new HashMap<String, T>();
    }

    public ErrorsSummaryProcessorAccumulator(T r, T... values){

        if (r instanceof ErrorSummaryerrsByCount) {
            String errorCode =  ((ErrorSummaryerrsByCount) r).error;
            if (internalMap.containsKey(errorCode))
                ((ErrorSummaryerrsByCount)internalMap.get(errorCode)).count.incrementAndGet();
            else
                internalMap.put(errorCode, r);
        }

        if (r instanceof ErrorSummaryerrsByUser) {
            String user =  ((ErrorSummaryerrsByUser) r).name;
            if (internalMap.containsKey(user)) {
                ErrorSummaryerrsByUser errorSummaryerrsByUser = ((ErrorSummaryerrsByUser) internalMap.get(user));
                errorSummaryerrsByUser.toterrors.incrementAndGet();
                String errorKey = (String) errorSummaryerrsByUser.errors.keySet().toArray()[0];
                if (errorSummaryerrsByUser.errors.containsKey(errorKey))
                    errorSummaryerrsByUser.errors.get(errorKey).count.incrementAndGet();
                else
                    errorSummaryerrsByUser.errors.put(errorKey, errorSummaryerrsByUser.errors.get(errorKey));
            }
            else
                internalMap.put(user, r);
        }

        if (r instanceof ErrorSummaryerrsBySite) {
            String user =  ((ErrorSummaryerrsBySite) r).name;
            if (internalMap.containsKey(user)) {
                ErrorSummaryerrsBySite errorSummaryerrsBySite = ((ErrorSummaryerrsBySite) internalMap.get(user));
                errorSummaryerrsBySite.toterrors.incrementAndGet();
                String errorKey = (String) errorSummaryerrsBySite.errors.keySet().toArray()[0];
                if (errorSummaryerrsBySite.errors.containsKey(errorKey))
                    errorSummaryerrsBySite.errors.get(errorKey).count.incrementAndGet();
                else
                    errorSummaryerrsBySite.errors.put(errorKey, errorSummaryerrsBySite.errors.get(errorKey));
            }
            else
                internalMap.put(user, r);
        }

        if (r instanceof ErrorSummaryerrsByTask) {
            String taskid =  ((ErrorSummaryerrsByTask) r).name;
            if (internalMap.containsKey(taskid)) {
                ErrorSummaryerrsByTask errorSummaryerrsByTask = ((ErrorSummaryerrsByTask) internalMap.get(taskid));
                errorSummaryerrsByTask.toterrors.incrementAndGet();
                String errorKey = (String) errorSummaryerrsByTask.errors.keySet().toArray()[0];
                if (errorSummaryerrsByTask.errors.containsKey(errorKey))
                    errorSummaryerrsByTask.errors.get(errorKey).count.incrementAndGet();
                else
                    errorSummaryerrsByTask.errors.put(errorKey, errorSummaryerrsByTask.errors.get(errorKey));
            }
            else
                internalMap.put(taskid, r);
        }








    }

/*

                errsByCount.error = errcode;
                errsByCount.codename = err.get("error");
                errsByCount.codeval = errnum;
                errsByCount.diag = errdiag;
                errsByCount.count = 0;



     */



/*
    public static final MaxIndexRowsAccumulatorParam PARAM_INSTANCE = new MaxIndexRowsAccumulatorParam();
    public static final ErrorsSummaryProcessorAccumulator ZERO = new ErrorsSummaryProcessorAccumulator();

    private int numCols = 0;
    private double[] maxVal;
    private int[] maxIndexes;

    private ErrorsSummaryProcessorAccumulator(){
        this.numCols = 0;
        maxVal = new double[numCols];
        maxIndexes = new int[numCols];



    }

    private ErrorsSummaryProcessorAccumulator(int numCols){
        this.numCols = numCols;
        maxVal = new double[numCols];
        maxIndexes = new int[numCols];
        for (int i = 0; i < numCols; i++) {
            maxVal[i] = Double.MIN_VALUE;
            maxIndexes[i] = 0;
        }
    }

    public ErrorsSummaryProcessorAccumulator(IndexedRow r, IndexedRow... values){
        if (numCols == 0) {
            numCols = r.vector().size();
            maxVal = new double[numCols];
            maxIndexes = new int[numCols];
            for (int i = 0; i < numCols; i++) {
                maxVal[i] = Double.MIN_VALUE;
                maxIndexes[i] = 0;
            }
        }

        double[] tmpVector = r.vector().toArray();

        for (int j = 0; j < tmpVector.length; j++)
            if (maxVal[j] < tmpVector[j]) {
                maxVal[j] = tmpVector[j];
                maxIndexes[j] = (int) r.index();
            }

        for (int i = 0; i < values.length; i++) {
            tmpVector = values[i].vector().toArray();
            for (int j = 0; j < tmpVector.length; j++)
                if (maxVal[j] < tmpVector[j]) {
                    maxVal[j] = tmpVector[j];
                    maxIndexes[j] = (int) values[i].index();
                }
        }
    }

    private ErrorsSummaryProcessorAccumulator(ErrorsSummaryProcessorAccumulator r1, ErrorsSummaryProcessorAccumulator r2){
        if (numCols == 0) {
            numCols = r2.numCols;
            maxVal = new double[numCols];
            maxIndexes = new int[numCols];
            for (int i = 0; i < numCols; i++) {
                maxVal[i] = Double.MIN_VALUE;
                maxIndexes[i] = 0;
            }
        }

        if (r1.numCols == 0) {
            for (int j = 0; j < r2.numCols; j++) {
                if (maxVal[j] < r2.maxVal[j]) {
                    maxVal[j] = r2.maxVal[j];
                    maxIndexes[j] = r2.maxIndexes[j];
                }
            }
        }
        else
            for (int j = 0; j < r2.numCols; j++)
            {
                if (r1.maxVal[j] < r2.maxVal[j]) {
                    if (maxVal[j] < r2.maxVal[j]) {
                        maxVal[j] = r2.maxVal[j];
                        maxIndexes[j] = r2.maxIndexes[j];
                    }
                }
                else {
                    if (maxVal[j] < r1.maxVal[j]) {
                        maxVal[j] = r1.maxVal[j];
                        maxIndexes[j] = r1.maxIndexes[j];
                    }
                }
            }
    }

    private ErrorsSummaryProcessorAccumulator(IndexedRow r1, ErrorsSummaryProcessorAccumulator r2) {

        if (numCols == 0) {
            numCols = r1.vector().size();
            maxVal = new double[numCols];
            maxIndexes = new int[numCols];
            for (int i = 0; i < numCols; i++) {
                maxVal[i] = Double.MIN_VALUE;
                maxIndexes[i] = 0;
            }
        }

        double[] tmpVector = r1.vector().toArray();

        for (int j = 0; j < numCols; j++)
        {
            if (tmpVector[j] < r2.maxVal[j]) {
                if (maxVal[j] < r2.maxVal[j]) {
                    maxVal[j] = r2.maxVal[j];
                    maxIndexes[j] = r2.maxIndexes[j];
                }
            }
            else {
                if (maxVal[j] < tmpVector[j]) {
                    maxVal[j] = tmpVector[j];
                    maxIndexes[j] = (int) r1.index();
                }
            }
        }
    }

    public ErrorsSummaryProcessorAccumulator add(IndexedRow d) {
        return new ErrorsSummaryProcessorAccumulator(d, this);
    }
    public ErrorsSummaryProcessorAccumulator add(ErrorsSummaryProcessorAccumulator d) {
        return new ErrorsSummaryProcessorAccumulator(this, d);
    }
    public int[] getIndexes() {
        return maxIndexes;
    }
    public double[] getMaxVal() {
        return maxVal;
    }

*/
    public static class MaxIndexRowsAccumulatorParam implements AccumulatorParam<ErrorsSummaryProcessorAccumulator>, Serializable {
        // only use PARAM_INSTANCE
        private MaxIndexRowsAccumulatorParam() {}

        public ErrorsSummaryProcessorAccumulator addAccumulator(final ErrorsSummaryProcessorAccumulator r, final ErrorsSummaryProcessorAccumulator t) {
            return null; //r.add(t);
        }

        public ErrorsSummaryProcessorAccumulator addInPlace(final ErrorsSummaryProcessorAccumulator r, final ErrorsSummaryProcessorAccumulator t) {
            return null; //r.add(t);
        }

        public ErrorsSummaryProcessorAccumulator zero(final ErrorsSummaryProcessorAccumulator initialValue) {
            return null; //ErrorsSummaryProcessorAccumulator.ZERO.add(initialValue);
        }

    }

}




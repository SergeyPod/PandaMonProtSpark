/**
 * Created by spadolski on 2/16/16.
 */

import breeze.optimize.linear.LinearProgram;
import org.apache.log4j.receivers.db.dialect.SybaseDialect;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.util.SystemClock;

import java.util.*;


public class RowDataModifier {
    Row oldRow;
    Map<String, Object> newValues = new HashMap<String, Object>();
    public String extraFields[] = new String[]{"ERRORINFO" , "HOMECLOUD", "JOBINFO", "OUTPUTFILETYPE", "JOBMODE", "SUBSTATE"}; // !!!Correspondent in DataPreprocessor

    RowDataModifier(Row oldRow){
        this.oldRow = oldRow;
    }

    void addVal(String name, Object val) {

        if ((!Arrays.asList(extraFields).contains(name)) && (!Arrays.asList(oldRow.schema().fieldNames()).contains(name))) {
            //System.out.println("Critical Error. Schema violation. RowDataModifier::addVal. " + name);
            //System.exit(0);
        } else
            newValues.put(name, val);

        //if (oldRow.schema().fields()[ oldRow.schema().fieldIndex(name)  ].dataType().  )

    }


    Row compileRow(){

        Map<String, Integer> fieldsMap = new HashMap<String, Integer>();
        int counter = 0;

        Object tmpArr[] = new Object[oldRow.length()+extraFields.length];

        //Object tmpArr[] = new Object[oldRow.length()];

        for (counter = 0; counter < oldRow.length(); counter++) {
            fieldsMap.put( oldRow.schema().fieldNames()[counter] , counter);

            Object dataInCell = oldRow.get(counter);
            if (oldRow.isNullAt(counter)) {
                //dataInCell = oldRow.schema().fields()[counter].dataType().asNullable();
                tmpArr[counter] = null;
            }
            else
                tmpArr[counter] = dataInCell;
        }


        for (int extraField = 0; extraField < extraFields.length; extraField++) {
            fieldsMap.put(  extraFields[extraField], extraField+counter );
            tmpArr[extraField+counter] = null;
        }


        Iterator it = newValues.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
           // System.out.println(pair.getKey() + " " + fieldsMap.get((String) pair.getKey()) + " " + pair.getValue());
            tmpArr[fieldsMap.get(pair.getKey())] = pair.getValue();
        }

        //System.out.println(Arrays.toString(tmpArr));
        return RowFactory.create(tmpArr);
    }
}

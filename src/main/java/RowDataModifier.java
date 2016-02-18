/**
 * Created by spadolski on 2/16/16.
 */

import breeze.optimize.linear.LinearProgram;
import org.apache.log4j.receivers.db.dialect.SybaseDialect;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class RowDataModifier {
    Row oldRow;
    Map<String, Object> newValues = new HashMap<String, Object>();
    public static String extraFields[] = new String[]{"ERRORINFO" , "HOMECLOUD", "JOBINFO", "OUTPUTFILETYPE"};

    RowDataModifier(Row oldRow){
        this.oldRow = oldRow;
    }

    void addVal(String name, Object val) {
        newValues.put(name, val);
    }

    Row compileRow(){

        ArrayList<Object> objects = new ArrayList<Object>(oldRow.length()+extraFields.length);

        Map<String, Integer> fieldsMap = new HashMap<String, Integer>();

        int counter = 0;

        for (counter = 0; counter < oldRow.length(); counter++) {
            objects.add(oldRow.get(counter));
            fieldsMap.put( oldRow.schema().fieldNames()[counter] , counter);
        }

        for (int extraField = 0; extraField < extraFields.length; extraField++) {
            objects.add(null);
            fieldsMap.put(  extraFields[extraField], extraField+counter );
        }

        Iterator it = newValues.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            //System.out.println(pair.getKey() + " " + fieldsMap.get((String) pair.getKey()) + " " + pair.getValue());
            objects.set( fieldsMap.get((String) pair.getKey()), pair.getValue());
        }

        return RowFactory.create(objects.toArray());
    }
}

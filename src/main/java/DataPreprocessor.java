import breeze.optimize.linear.LinearProgram;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.math.Ordering;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by spadolski on 2/16/16.
 */
public class DataPreprocessor {

    Map<String,String> homeClouds;

    DataPreprocessor() {
        Gson gson = new Gson();
        Type mapOfStringObjectType = new TypeToken<Map<String,String>>() {}.getType();
        homeClouds = gson.fromJson(CONSTS.jSONClouds, mapOfStringObjectType);
    }


    DataFrame preprocess(DataFrame oldDF, String tableName){
        DataFrame retDF = null;
        if (tableName.contentEquals("ATLAS_PANDABIGMON.COMBINED_WAIT_ACT_DEF_ARCH4")) {
            //retDF.
            JavaRDD<Row> modifiedRDD = oldDF.toJavaRDD().map(new DistribPreProcessor(homeClouds));
            StructType schema = oldDF.schema();

            schema = schema.add("ERRORINFO", DataTypes.StringType, true);
            schema = schema.add("HOMECLOUD", DataTypes.StringType, true);
            schema = schema.add("JOBINFO", DataTypes.StringType, true);
            schema = schema.add("OUTPUTFILETYPE", DataTypes.StringType, true);
            retDF = oldDF.sqlContext().createDataFrame( modifiedRDD , schema);
        }
        else
            retDF = oldDF;
        return retDF;

    }

}

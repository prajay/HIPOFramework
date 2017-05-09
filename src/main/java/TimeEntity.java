import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Objects;

/**
 * Created by dreamlab2 on 4/18/17.
 */
public class TimeEntity extends Entity {
    public TimeEntity(JavaRDD inputRDD, Object hintObject, SelectInterface<HashMap<String, Object>, HashMap<String, Object>> selectInterface, FilterInterface<HashMap<String, Object>> filterInterface, PartitionInterface<HashMap<String, Object>> partitionInterface, CompareInterface<HashMap<String, Object>> compareInterface, String partitionKeys) {
        super(inputRDD, hintObject, selectInterface, filterInterface, partitionInterface, compareInterface, partitionKeys);
    }

    @Override
    protected JavaRDD<HashMap<String, Object>> generateEntityRDD(JavaRDD<String> inputRDD) {
        JavaRDD<HashMap<String,Object>> timeRDD = inputRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("Node");
//                return s._1().toString().equals("Node");
            }
        }).map(new Function<String, HashMap<String, Object>>() {
            @Override
            public HashMap<String, Object> call(String s) throws Exception {
//                HashMap m = s._2();
//                HashMap<String,Object> h = new HashMap<String, Object>();
//                h.put("Timestamp", m.get("graph_timestamp_start"));

                String[] values = s.split(",");
                HashMap<String, Object> h = new HashMap<>();
                for (int i = 0; i < values.length; i++) {
                    String[] pairs = values[i].split(" ");
                    if (pairs[0].equals("graph_timestamp_start")) {
                        h.put("TimeStamp", new Double(Double.parseDouble(pairs[1])).longValue());
                    }
                }
                return h;
            }
        }).distinct();
        return timeRDD;
    }

    @Override
    public String getJoinCondition(Entity E) {
        return "Timestamp";
    }
}

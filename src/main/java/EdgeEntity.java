import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.HashMap;

/**
 * Created by dreamlab2 on 4/18/17.
 */
public class EdgeEntity extends Entity {
    public EdgeEntity(JavaRDD inputRDD, Object hintObject, SelectInterface<HashMap<String, Object>, HashMap<String, Object>> selectInterface, FilterInterface<HashMap<String, Object>> filterInterface, PartitionInterface<HashMap<String, Object>> partitionInterface, CompareInterface<HashMap<String, Object>> compareInterface, String partitionKeys) {
        super(inputRDD, hintObject, selectInterface, filterInterface, partitionInterface, compareInterface, partitionKeys);
    }

    @Override
    protected JavaRDD<HashMap<String, Object>> generateEntityRDD(JavaRDD<String> inputRDD) {
        JavaRDD<HashMap<String,Object>> edgeRDD = inputRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return s.contains("Edge");
//                return s._1().toString().equals("Edge");
            }
        }).map(new Function<String, HashMap<String, Object>>() {
            @Override
            public HashMap<String, Object> call(String s) throws Exception {
//                HashMap m = s._2();
                String[] values = s.split(",");
                HashMap<String, Object> h = new HashMap<>();
                for (int i = 0; i < values.length; i++) {
                    String[] pairs = values[i].split(" ");

                    switch (pairs[0]) {
                        case "graph_timestamp_start":
                            h.put("TimeStamp", new Double(Double.parseDouble(pairs[1])).longValue());
                            break;
                        case "source":
                            h.put("Source", Long.parseLong(pairs[1]));
                            break;
                        case "target":
                            h.put("Target", Long.parseLong(pairs[1]));
                            break;
                        case "id":
                            h.put("EdgeId", Long.parseLong(pairs[1]));
                            break;
                    }
                }
                return h;
            }
        });
        edgeRDD.persist(StorageLevel.DISK_ONLY());
        return edgeRDD;
    }

    @Override
    public String getJoinCondition(Entity E) {
        switch (E.getClass().getSimpleName()) {
            case "TimeEntity":
                return "TimeStamp";
            case "VertexEntity":
                return "TimeStamp,Source";
            case "VertexPropertyEntity":
                return "TimeStamp,Source";
            case "EdgePropertyEntity":
                return "TimeStamp,EdgeId";
            default:
                return "TimeStamp,Source";
        }
    }
}

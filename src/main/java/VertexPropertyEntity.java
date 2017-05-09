import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by dreamlab2 on 4/18/17.
 */
public class VertexPropertyEntity extends Entity {
    public VertexPropertyEntity(JavaRDD inputRDD, Object hintObject, SelectInterface<HashMap<String, Object>, HashMap<String, Object>> selectInterface, FilterInterface<HashMap<String, Object>> filterInterface, PartitionInterface<HashMap<String, Object>> partitionInterface, CompareInterface<HashMap<String, Object>> compareInterface, String partitionKeys) {
        super(inputRDD, hintObject, selectInterface, filterInterface, partitionInterface, compareInterface, partitionKeys);
    }

    @Override
    protected JavaRDD<HashMap<String, Object>> generateEntityRDD(JavaRDD<String> inputRDD) {
        JavaRDD<HashMap<String,Object>> vertexpropertyRDD = inputRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
//                return s._1().toString().equals("Node");
                return s.contains("Node");
            }
        }).map(new Function<String, HashMap<String, Object>>() {
            @Override
            public HashMap<String, Object> call(String s) throws Exception {
//                HashMap m = s._2();
//                HashMap<String, Object> h = new HashMap<>();
//                h.put("Timestamp", Long.parseLong(m.get("graph_timestamp_start").toString()));
//                h.put("SubgraphId", Integer.parseInt(m.get("sgid").toString()));
//                h.put("VertexId", Long.parseLong(m.get("id").toString()));
//                Iterator it = m.entrySet().iterator();
//                while (it.hasNext()) {
//                    Map.Entry pair = (Map.Entry) it.next();
//                    int i = 1;
//                    if (pair.getKey().toString().equals("graph_timestamp_start") || pair.getKey().toString().equals("sgid") || pair.getKey().toString().equals("sgid")) {
//                        continue;
//                    } else {
//                        h.put("Prop" + i, pair.getValue());
//                    }
//                }

                String[] values = s.split(",");
                int j = 1;
                HashMap<String, Object> h = new HashMap<>();
                for (int i = 0; i < values.length; i++) {
                    String[] pairs = values[i].split(" ");

                    switch (pairs[0]) {
                        case "graph_timestamp_start":
                            h.put("TimeStamp", new Double(Double.parseDouble(pairs[1])).longValue());
                            break;
                        case "sgid":
                            h.put("SubgraphId", Integer.parseInt(pairs[1]));
                            break;
                        case "id":
                            h.put("VertexId", Long.parseLong(pairs[1]));
                            break;
                        default:
                            h.put("Prop" + j, pairs[1]);
                            j++;
                            break;
                    }
                }

                return h;
            }
        });
        return vertexpropertyRDD;
    }

    @Override
    public String getJoinCondition(Entity E) {
        switch (E.getClass().getSimpleName()) {
            case "TimeEntity":
                return "TimeStamp";
            default:
                return "VertexId";
        }
    }
}

import java.util.HashMap;

/**
 * Created by dreamlab2 on 4/19/17.
 */
public class VertexCompareInterface implements CompareInterface<HashMap<String,Object>> {
    @Override
    public int compare(HashMap<String,Object> r1, HashMap<String,Object> r2) {
        if (((Integer)r1.get("SubgraphId")).compareTo((Integer) r2.get("SubgraphId")) != 0) {
            return ((Integer) r1.get("SubgraphId")).compareTo((Integer) r2.get("SubgraphId"));
        } else {
            return ((Long) r1.get("VertexId")).compareTo((Long) r2.get("VertexId"));
        }
    }
}

import java.util.HashMap;

/**
 * Created by dreamlab2 on 4/19/17.
 */
public class EdgePartitionInterface implements PartitionInterface<HashMap<String,Object>> {
    @Override
    public int numPartitions() {
        return 1;
    }

    @Override
    public int getPartitionId(HashMap<String, Object> stringObjectHashMap, Object hint) {
        return 0;
    }
}

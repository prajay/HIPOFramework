import java.io.Serializable;

/**
 * Created by dreamlab2 on 4/14/17.
 */
public interface PartitionInterface<R> extends Serializable {
    int numPartitions();
    int getPartitionId(R r, Object hint);
}

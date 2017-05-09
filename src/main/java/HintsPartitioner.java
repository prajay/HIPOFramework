import org.apache.spark.Partitioner;

import java.util.*;

/**
 * Created by dreamlab2 on 4/4/17.
 */
public class HintsPartitioner extends Partitioner {
    List<Object> hints;
    List<PartitionInterface<HashMap<String, Object>>> partitioners;

    public HintsPartitioner(ArrayList<PartitionInterface<HashMap<String, Object>>> partitionInterfaces, ArrayList<Object> hints) {
        this.hints = hints;
        this.partitioners = partitionInterfaces;
    }

    @Override
    public int numPartitions() {
        Iterator<PartitionInterface<HashMap<String, Object>>> it = partitioners.iterator();
        int numParts = 1;
        while (it.hasNext()) {
            numParts = numParts * (it.next().numPartitions());
        }
        return numParts;
    }

    @Override
    public int getPartition(Object key) {
        Iterator<PartitionInterface<HashMap<String, Object>>> it = partitioners.iterator();
        Iterator it1 = hints.iterator();
        int partitionId = 0;
        while (it.hasNext()) {
            if (partitionId == 0) {
                PartitionInterface<HashMap<String, Object>> p = it.next();
                partitionId = p.getPartitionId((HashMap<String, Object>) key, it1.next());
                //System.out.println("Partition class: " + p.getClass().getName() + " " + partitionId);
            }
            else {
                PartitionInterface<HashMap<String, Object>> p = it.next();
                int mult = p.numPartitions() > 1 ? 2 : 1;
                partitionId = mult * (partitionId + p.getPartitionId((HashMap<String, Object>) key, it1.next()));
                //System.out.println("Oh Partition class: " + p.getClass().getName() + " " + partitionId);
            }
        }

        return partitionId;
    }

}

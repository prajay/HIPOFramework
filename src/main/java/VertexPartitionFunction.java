import java.lang.Long;

import scala.Tuple2;
import scala.Tuple3;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by dreamlab2 on 4/11/17.
 */
public class VertexPartitionFunction //extends PartitionFunction<Tuple2<Long,Long>, Tuple3<Long,Long,Integer>> {
{

    public class Distance {
        Long subgraphId;
        Integer weight;

        public Distance(Long subgraphId, Integer weight) {
            this.subgraphId = subgraphId;
            this.weight = weight;
        }

        public Long getSubgraphId() {
            return subgraphId;
        }

        public Integer getWeight() {
            return weight;
        }

        public void setSubgraphId(Long subgraphId) {
            this.subgraphId = subgraphId;
        }

        public void setWeight(Integer weight) {
            this.weight = weight;
        }
    }

    public VertexPartitionFunction(Object hint, PartitionInterface<Tuple2<Long,Long>> partitionInterface) {
        //super(hint, partitionInterface);
    }

//    @Override
//    protected void generatePartitionMapping() {
//        //Generate random seeds for BFS (number of seeds = numParts)
//        Long[] seeds = new Long[10];
//        HashMap<Long,HashMap<Long,Integer>> distanceMap = new HashMap<Long,HashMap<Long, Integer>>();
//        for (int i = 0; i < 10; i++) {
//            seeds[i] = ThreadLocalRandom.current().nextLong((long)0, (long)100);
//            Queue<Distance> queue = new LinkedList<Distance>();
//            Distance d = new Distance(seeds[i],0);
//            queue.add(d);
//            while (!queue.isEmpty()) {
//                Distance element = queue.remove();
//                Long subgraphId = element.getSubgraphId();
//                Integer weight = element.getWeight();
//                HashMap<Long,Integer> h = new HashMap<Long,Integer>();
//                h.put(seeds[i],weight);
//                if (!distanceMap.containsKey(subgraphId)) {
//                    distanceMap.put(subgraphId,h);
//                }
//                weight++;
//                VertexHints vh = (VertexHints) hint;
//                Iterator<Long> it = vh.getSubgraphMap().get(subgraphId).iterator();
//                while (it.hasNext()) {
//                    Distance d1 = new Distance(it.next(), weight);
//                    queue.add(d1);
//                }
//            }
//        }

//        Iterator it = distanceMap.entrySet().iterator();
//        while (it.hasNext())
//        {
//            Map.Entry<Long,HashMap<Long,Integer>> pair = (Map.Entry<Long, HashMap<Long, Integer>>) it.next();
//            Integer min = 999;
//            for (int i = 0; i < 10; i++) {
//                if (pair.getValue().get(seeds[i]) < min) {
//                    min = pair.getValue().get(seeds[i]);
//                    partitionMap.put(pair.getKey(),i);
//                }
//            }
//        }
//    }

//    @Override
//    public Tuple3<Long, Long, Integer> call(Tuple2<Long, Long> v1) {
//        return new Tuple3<Long,Long,Integer>(v1._1(),v1._2(),partitionMap.get(v1._1()));
//    }
}

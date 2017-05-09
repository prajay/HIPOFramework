import java.util.*;

/**
 * Created by dreamlab2 on 4/19/17.
 */
public class VertexPartitionInterface implements PartitionInterface<HashMap<String,Object>> {
    @Override
    public int numPartitions() {
        return 10;
    }

    @Override
    public int getPartitionId(HashMap<String, Object> stringObjectHashMap, Object hint) {
        hint = (VertexHints) hint;
        Integer subgraphId = (Integer) stringObjectHashMap.get("SubgraphId");
        int partid = 0;
        Queue<Integer> queue = new LinkedList<Integer>();
        Integer[] seeds = ((VertexHints) hint).getRandomSeeds();

        //System.out.println(seeds);
        HashMap<Integer,List<Integer>> subgraphMap = (HashMap<Integer, List<Integer>>) ((VertexHints) hint).getSubgraphMap();
        queue.add(subgraphId);
        while (!queue.isEmpty()) {
            Integer el = queue.remove();
            List<Integer> neighbours = subgraphMap.get(el);
            for (int i = 0; i < seeds.length; i++) {
                if (neighbours.contains(seeds[i])) {
                    partid = i;
                    return partid;
                }
            }
            Iterator<Integer> it = neighbours.iterator();
            while (it.hasNext()) {
                queue.add(it.next());
            }
        }
        return partid;
    }
}

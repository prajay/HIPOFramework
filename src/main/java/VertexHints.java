import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by dreamlab2 on 4/4/17.
 */
public class VertexHints implements Serializable {
    Map<Integer, List<Integer>> subgraphMap;
    Integer[] randomSeeds;


    public VertexHints(HashMap<Integer, List<Integer>> subgraphMap) {
        this.subgraphMap = subgraphMap;
        randomSeeds = new Integer[10];
        int size = subgraphMap.size();
        for (int i = 0; i < 10; i++) {
            int random = ThreadLocalRandom.current().nextInt(0, size);
//            System.out.println(random);
            randomSeeds[i] = random;
        }

    }

    public Integer[] getRandomSeeds() {
        return randomSeeds;
    }

    public Map<Integer, List<Integer>> getSubgraphMap() {
        return subgraphMap;
    }

    public void setSubgraphMap(Map<Integer, List<Integer>> subgraphMap) {
        this.subgraphMap = subgraphMap;
    }
}


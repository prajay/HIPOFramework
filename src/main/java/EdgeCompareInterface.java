import java.util.HashMap;

/**
 * Created by dreamlab2 on 4/19/17.
 */
public class EdgeCompareInterface implements CompareInterface<HashMap<String, Object>> {
    @Override
    public int compare(HashMap<String, Object> r1, HashMap<String, Object> r2) {
        if (r1.get("EdgeId") == null && r2.get("EdgeId") != null) {
            return -1;
        } else if (r1.get("EdgeId") != null && r2.get("EdgeId") == null) {
            return 1;
        } else if (r1.get("EdgeId") == null && r2.get("EdgeId") == null) {
            return 0;
        } else {
            return ((Long) r1.get("EdgeId")).compareTo((Long) r2.get("EdgeId"));
        }
    }
}

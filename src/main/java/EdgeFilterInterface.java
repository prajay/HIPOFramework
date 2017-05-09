import java.util.HashMap;

/**
 * Created by dreamlab2 on 4/19/17.
 */
public class EdgeFilterInterface implements FilterInterface<HashMap<String,Object>> {
    @Override
    public boolean filter(HashMap<String, Object> stringObjectHashMap) {
        return true;
    }
}

import java.util.HashMap;

/**
 * Created by dreamlab2 on 4/19/17.
 */
public class EdgeSelectInterface implements SelectInterface<HashMap<String,Object>,HashMap<String,Object>> {
    @Override
    public HashMap<String, Object> select(HashMap<String, Object> stringObjectHashMap) {
        return stringObjectHashMap;
    }
}

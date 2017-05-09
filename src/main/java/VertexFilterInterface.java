import java.util.HashMap;

/**
 * Created by dreamlab2 on 4/19/17.
 */
public class VertexFilterInterface implements FilterInterface<HashMap<String,Object>> {
    @Override
    public boolean filter(HashMap<String,Object> hashMap) {
        return true;
    }
}

import java.io.Serializable;
import java.util.List;

/**
 * Created by dreamlab2 on 4/4/17.
 */
public class PropertyHints implements Serializable {
    List<List<String>> propNames;

    public PropertyHints(List<List<String>> propNames) {
        this.propNames = propNames;
    }

    public List<List<String>> getPropNames() {
        return propNames;
    }

    public void setPropNames(List<List<String>> propNames) {
        this.propNames = propNames;
    }
}

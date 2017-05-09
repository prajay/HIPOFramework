import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by dreamlab2 on 4/14/17.
 */
public interface CompareInterface<R> extends Serializable, Comparator<R> {
    int compare(R r1, R r2);
}

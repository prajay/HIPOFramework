import java.io.Serializable;

/**
 * Created by dreamlab2 on 4/14/17.
 */
public interface FilterInterface<R> extends Serializable {
    boolean filter(R r);
}

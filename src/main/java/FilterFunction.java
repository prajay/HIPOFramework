import org.apache.spark.api.java.function.Function;

/**
 * Created by dreamlab2 on 4/14/17.
 */
public class FilterFunction<T1> implements Function<T1,Boolean> {
    FilterInterface<T1> filterInterface;

    public FilterFunction(FilterInterface<T1> filterInterface) {
        this.filterInterface = filterInterface;
    }

    @Override
    public Boolean call(T1 v1) throws Exception {
        return filterInterface.filter(v1);
    }
}

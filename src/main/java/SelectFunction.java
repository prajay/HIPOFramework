import org.apache.spark.api.java.function.Function;

/**
 * Created by dreamlab2 on 4/14/17.
 */
public class SelectFunction<T1,R> implements Function<T1,R> {

    SelectInterface<T1,R> selectInterface;

    public SelectFunction(SelectInterface<T1, R> selectInterface) {
        this.selectInterface = selectInterface;
    }

    @Override
    public R call(T1 v1) throws Exception {
        return selectInterface.select(v1);
    }
}

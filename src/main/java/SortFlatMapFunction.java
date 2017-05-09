import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by dreamlab2 on 4/12/17.
 */
public class SortFlatMapFunction<T1 extends Iterator, R> implements FlatMapFunction<T1, R> {

    CompareInterface<R> compareInterface;

    public SortFlatMapFunction(CompareInterface<R> compareInterface) {
        this.compareInterface = compareInterface;
    }

    @Override
    public Iterator<R> call(T1 t1) throws Exception {
        ArrayList<R> arrayList = new ArrayList<R>();
        while (t1.hasNext()) {
            arrayList.add((R) t1.next());
        }
        Collections.sort(arrayList, this.compareInterface);
        return arrayList.iterator();
    }
}

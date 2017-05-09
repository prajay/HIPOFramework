import org.apache.spark.api.java.function.Function;
import scala.Product;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by dreamlab2 on 4/11/17.
 */
public class PartitionFunction<T1 extends HashMap,R extends HashMap> implements Function<T1,R> {
    Object hint;
    PartitionInterface<T1> partitionInterface;

    public PartitionFunction(Object hint, PartitionInterface<T1> partitionInterface) {
        this.hint = hint;
        this.partitionInterface = partitionInterface;
    }

    public void setHint(Object hint) {
        this.hint = hint;
    }

    public Object getHint() {
        return hint;
    }

    public R call(T1 v1) {
        //return partitionInterface.getPartitionId(v1);
        Integer partId = partitionInterface.getPartitionId(v1, hint);
//        String inputClass = v1.getClass().getName();
//        Pattern p = Pattern.compile("^.*(\\d+)$");
//        Integer cols = Integer.parseInt(p.matcher(inputClass).group(1));
//        cols++;
//        Class<R> cls = (Class<R>) Class.forName("scala.Tuple" + cols);
//        Iterator it = v1.entrySet().iterator();
//        Class[] classes = new Class[cols];
//        Object[] values = new Object[cols];
//        int i = 0;
//        while (it.hasNext()) {
//            Map.Entry pair = (Map.Entry) it.next();

//            classes[i] = value.getClass();
//            values[i] = value;
//            i++;
//        }
//        classes[i] = Integer.class;
//        values[i] = partitionInterface.getPartitionId(v1);

//        Constructor<R> cons = cls.getDeclaredConstructor(classes);
//        R r = cons.newInstance(values);
        v1.put("partId", partId);
        return (R) v1;
    }
}

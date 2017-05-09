import scala.Tuple3;
import scala.Tuple4;

import java.util.Iterator;

/**
 * Created by dreamlab2 on 4/13/17.
 */
public class VertexSortFlatMapFunction extends SortFlatMapFunction<Iterator<Tuple3<Long,Long,Integer>>, Tuple3<Long,Long,Integer>> {

    public VertexSortFlatMapFunction(CompareInterface<Tuple3<Long, Long, Integer>> compareInterface) {
        super(compareInterface);
    }

//    @Override
//    public int compare(Tuple3<Long, Long, Integer> r1, Tuple3<Long, Long, Integer> r2) {
//        if (r1._1() != r2._1())
//            return (int)(r1._1() - r2._1());
//        else
//            return (int)(r1._2() - r2._2());
//    }
}

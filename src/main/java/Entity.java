import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by dreamlab2 on 4/13/17.
 */
public abstract class Entity implements Serializable {
    JavaRDD<HashMap<String, Object>> RDD;
    Function<HashMap<String, Object>, HashMap<String, Object>> Select;
    Function<HashMap<String, Object>, Boolean> Filter;
    //PartitionFunction<B, C> Partition;
    PartitionInterface<HashMap<String, Object>> partitionInterface;
    Object hint;
    String partitionKeys;
    SortFlatMapFunction<Iterator<HashMap<String, Object>>, HashMap<String, Object>> Order;
    List<Entity> children;
//    ArrayList<JavaRDD<C>> partitionedRDDs;

    public Entity(JavaRDD inputRDD, Object hintObject, SelectInterface<HashMap<String, Object>, HashMap<String, Object>> selectInterface, FilterInterface<HashMap<String, Object>> filterInterface, PartitionInterface<HashMap<String, Object>> partitionInterface, CompareInterface<HashMap<String, Object>> compareInterface, String partitionKeys) {
        this.RDD = generateEntityRDD(inputRDD);
        this.Select = (selectInterface == null) ? null : new SelectFunction<HashMap<String, Object>, HashMap<String, Object>>(selectInterface);
        this.Filter = (filterInterface == null) ? null : new FilterFunction<HashMap<String, Object>>(filterInterface);
        //this.Partition = new PartitionFunction<B, C>(hintObject, partitionInterface);
        this.partitionInterface = partitionInterface;
        this.hint = hintObject;
        this.Order = new SortFlatMapFunction<Iterator<HashMap<String, Object>>, HashMap<String, Object>>(compareInterface);
        this.children = new ArrayList<>();
        this.partitionKeys = partitionKeys;
//        this.partitionedRDDs = new ArrayList<JavaRDD<HashMap>>();

        //System.out.println("Hello");
    }

    public class partFunction implements Function<HashMap, Boolean> {
        Integer partId;

        public partFunction(Integer partId) {
            //System.out.println("Splitting RDD");
            this.partId = partId;
        }

        @Override
        public Boolean call(HashMap s) throws Exception {
            return partId.compareTo((Integer) s.get("partId")) == 0;
        }
    }

    public class newPairFunction implements PairFunction<HashMap<String, Object>, Object, HashMap<String, Object>> {
        String joinKey;

        public newPairFunction(String joinKey) {
            this.joinKey = joinKey;
        }

        @Override
        public Tuple2<Object, HashMap<String, Object>> call(HashMap<String, Object> s) throws Exception {
            HashMap<String, Object> h = s;
            if (joinKey.contains(",")) {
                String[] keys = joinKey.split(",");
//                System.out.println(keys[0] + " " + keys[1]);
                long k1 = (long) h.get(keys[0]);
                long k2 = (long) h.get(keys[1]);
                h.remove(k1);
                h.remove(k2);
                PairKey p = new PairKey(k1, k2);
                return new Tuple2(p, h);
            } else {
                Object k = h.get(joinKey);
                h.remove(k);
                return new Tuple2(k, h);
            }
        }
    }

    public class newMapFunction implements Function<Tuple2<Object, Tuple2<HashMap, Optional<HashMap>>>, HashMap<String, Object>> {
        String key;

        public newMapFunction(String key) {
            this.key = key;
        }

        @Override
        public HashMap call(Tuple2<Object, Tuple2<HashMap, Optional<HashMap>>> s) throws Exception {
            HashMap<String, Object> h = s._2()._1();
            HashMap<String, Object> h2 = new HashMap<>();
            if (!key.contains(",")) {
                h.put(key, s._1);
            } else {
                String[] keys = key.split(",");
                PairKey p = (PairKey) s._1();
                h.put(keys[0], p.ts);
                h.put(keys[1], p.id);
            }
            h2.putAll(h);
            if (s._2()._2().isPresent()) {
                h2.putAll(s._2()._2().get());
            }
            return h2;
        }
    }

    protected abstract JavaRDD<HashMap<String, Object>> generateEntityRDD(JavaRDD<String> inputRDD);

    public abstract String getJoinCondition(Entity E);

    public JavaRDD runHIPO() {
//    public List<JavaRDD> runHIPO() {
        JavaRDD<HashMap<String, Object>> newRDD = RDD.map(Select).filter(Filter);

//        newRDD = newRDD.map(Partition).persist(StorageLevel.DISK_ONLY());
        //newRDD.saveAsTextFile("/opt/jayanth/temp");


//        List<Integer> partitions = newRDD.map(new Function<C, Integer>() {
//            @Override
//            public Integer call(C s) throws Exception {
//                return Integer.parseInt(s.get("partId").toString());
//            }
//        }).distinct().collect();
//        Iterator<Integer> it = partitions.iterator();
//        List<JavaRDD> returnList = new ArrayList<JavaRDD>();
//        while (it.hasNext()) {
//            Integer partId = it.next();
//            System.out.println(partId);
//            JavaRDD<C> part = newRDD.filter(new partFunction(partId)).persist(StorageLevel.DISK_ONLY());
//            partitionedRDDs.add(part);
//        }
//        if (!children.isEmpty()) {
//            Iterator<JavaRDD<C>> it1 = partitionedRDDs.iterator();
//            Iterator<Entity> childit = children.iterator();
//            while (it1.hasNext()) {
//                while (childit.hasNext()) {
//                    Entity childEntity = childit.next();
//                    String joinKey = getJoinCondition(childEntity);
//                    List<JavaRDD> childRDDs = childEntity.runHIPO();
//
//                    JavaPairRDD parentpartitionedpair = convertToPair(it1.next(),joinKey).partitionBy(new HashPartitioner(4)).persist(StorageLevel.DISK_ONLY());
//                    Iterator<JavaRDD> it3 = childRDDs.iterator();
//                    while (it3.hasNext()) {
//                        JavaRDD childRDD = it3.next();
//                        String childJoinKey = childEntity.getJoinCondition(this);
//                        JavaPairRDD childpartitionedpair = childEntity.convertToPair(childRDD,childJoinKey).partitionBy( new HashPartitioner(4)).persist(StorageLevel.DISK_ONLY());
//                        returnList.add(convertToRDD(parentpartitionedpair.leftOuterJoin(childpartitionedpair), joinKey));
//                    }
//                }
//            }
//        } else {
//            Iterator<JavaRDD<C>> it1 = partitionedRDDs.iterator();
//            while (it1.hasNext()) {
//                returnList.add(it1.next());
//            }
//        }

        if (!children.isEmpty()) {
            Iterator<Entity> childit = children.iterator();

            while (childit.hasNext()) {
                Entity childEntity = childit.next();
                String joinKey = getJoinCondition(childEntity);
                JavaRDD childRDD = childEntity.runHIPO();
                JavaPairRDD parentpair = convertToPair(newRDD, joinKey);
//                parentpair.foreach(new VoidFunction<Tuple2>() {
//
//                    @Override
//                    public void call(Tuple2 tuple2) throws Exception {
//                        System.out.println("Parent " + ((PairKey) tuple2._1()).id + ((PairKey) tuple2._1()).ts);
//                    }
//                });
                String childJoinKey = childEntity.getJoinCondition(this);
                JavaPairRDD childpair = childEntity.convertToPair(childRDD, childJoinKey);

//                childpair.foreach(new VoidFunction<Tuple2>() {
//
//                    @Override
//                    public void call(Tuple2 tuple2) throws Exception {
//                        System.out.println("Child " + ((PairKey) tuple2._1()).id + ((PairKey) tuple2._1()).ts);
//                    }
//                });
                newRDD = convertToRDD(parentpair.leftOuterJoin(childpair, 6), joinKey);
            }
            return newRDD;
        } else {
            return newRDD;
        }
//        returnList = applysort((ArrayList<JavaRDD>) returnList);
    }

    public JavaRDD<HashMap<String, Object>> callPartitioner(JavaRDD<HashMap<String, Object>> rdd) {

        ArrayList<PartitionInterface<HashMap<String,Object>>> partitionInterfaces = new ArrayList<>();
        ArrayList<Object> hints = new ArrayList<>();
        String pkey = partitionKeys;
        partitionInterfaces.add(this.partitionInterface);
        hints.add(hint);
        if (!children.isEmpty()) {
            Iterator<Entity> childit = children.iterator();
            while (childit.hasNext()) {
                Entity childEntity = childit.next();
                partitionInterfaces.add(childEntity.partitionInterface);
                hints.add(childEntity.hint);
                pkey = pkey + "," + childEntity.partitionKeys;
            }
        }
        final String[] finalpkeys = pkey.split(",");
        JavaRDD<HashMap<String, Object>> newrdd = rdd.mapToPair(new PairFunction<HashMap<String, Object>, HashMap<String, Object>, HashMap<String, Object>>() {
            @Override
            public Tuple2<HashMap<String, Object>, HashMap<String, Object>> call(HashMap<String, Object> hashMap) throws Exception {
                HashMap<String, Object> h = new HashMap<>();
                for (int i = 0; i < finalpkeys.length; i++) {
                    h.put(finalpkeys[i], hashMap.remove(finalpkeys[i]));
                }
                return new Tuple2<HashMap<String, Object>, HashMap<String, Object>>(h, hashMap);
            }
        }).partitionBy(new HintsPartitioner(partitionInterfaces, hints)).map(new Function<Tuple2<HashMap<String, Object>, HashMap<String, Object>>, HashMap<String, Object>>() {
            @Override
            public HashMap<String, Object> call(Tuple2<HashMap<String, Object>, HashMap<String, Object>> v1) throws Exception {
                HashMap<String, Object> h = new HashMap<>();
                h.putAll(v1._1);
                h.putAll(v1._2);
                return h;
            }
        });

        return callSort(newrdd,this);
    }

    public JavaRDD<HashMap<String, Object>> callSort(JavaRDD<HashMap<String, Object>> rdd, Entity E) {
        if (E.children.isEmpty())
            return rdd.mapPartitions(E.Order).persist(StorageLevel.DISK_ONLY());
        else {
            Iterator<Entity> childit = E.getChildren().iterator();
            while (childit.hasNext()) {
                rdd = callSort(rdd, childit.next());
            }
            return rdd.mapPartitions(E.Order).persist(StorageLevel.DISK_ONLY());
        }
    }

    public ArrayList<JavaRDD> applysort(ArrayList<JavaRDD> list) {
        ArrayList<JavaRDD> newlist = new ArrayList<>();
        Iterator<JavaRDD> iterator = list.iterator();
        while (iterator.hasNext()) {
            JavaRDD r = iterator.next().mapPartitions(Order).persist(StorageLevel.DISK_ONLY());
            newlist.add(r);
        }
        return newlist;
    }

    public JavaPairRDD<Object, HashMap<String, Object>> convertToPair(JavaRDD<HashMap<String, Object>> RDD, String joinKey) {
        return RDD.mapToPair(new newPairFunction(joinKey)).repartition(6).persist(StorageLevel.DISK_ONLY());
    }

    public JavaRDD<HashMap<String, Object>> convertToRDD(JavaPairRDD<Object, Tuple2<HashMap, Optional<HashMap>>> pairRDD, String key) {
        return pairRDD.map(new newMapFunction(key));
    }

    public void addChild(Entity child) {
        children.add(child);
    }

    public List<Entity> getChildren() {
        return children;
    }
}



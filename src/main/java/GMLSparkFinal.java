import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.json.JSONObject;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple6;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


/**
 * Created by dreamlab2 on 4/11/17.
 */
public class GMLSparkFinal {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Spark Test").setMaster("local");
        conf.set("spark.driver.memory", "3g");
        conf.set("spark.executor.memory", "3g");
        conf.set("spark.executor.heartbeatInterval", "30s");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        Logger LOG = Logger.getLogger(GMLSparkFinal.class);
//        LOG.setLevel(Level.DEBUG);
        sc.setLogLevel("ERROR");
        Configuration jobConf = new Configuration();
//        JavaPairRDD<String, HashMap> input = sc.newAPIHadoopFile("/opt/jayanth/SGIDInstances/cit-Patents.txt-instance-1.gml", GMLInputFormat.class, Text.class, MapWritable.class, jobConf).mapToPair(new PairFunction<Tuple2<Text, MapWritable>, String, HashMap>() {
        JavaRDD<String> input = sc.newAPIHadoopFile("/opt/jayanth/SGIDInstances/cit-Patents.txt-instance-1.gml", GMLInputFormat.class, LongWritable.class, Text.class, jobConf).map(new Function<Tuple2<LongWritable, Text>, String>() {
            @Override
            public String call(Tuple2<LongWritable, Text> textMapWritableTuple2) throws Exception {
//                Iterator it = textMapWritableTuple2._2().entrySet().iterator();
//                HashMap<String, Object> h = new HashMap<>();
//                while (it.hasNext()) {
//                    Map.Entry<Text, Text> pair = (Map.Entry<Text, Text>) it.next();
//                    h.put(pair.getKey().toString(), pair.getValue().toString());
//                }
                return textMapWritableTuple2._2().toString();
//                return new Tuple2<String, HashMap>(textMapWritableTuple2._1().toString(), h);
            }
        }).repartition(4).persist(StorageLevel.DISK_ONLY());

        HashMap<Integer, List<Integer>> metaGraphHints = null;
        try {
            metaGraphHints = generateMetaGraph();
        } catch (IOException e) {
            e.printStackTrace();
        }

        VertexHints vh = new VertexHints(metaGraphHints);
        VertexEntity v = new VertexEntity(input, vh, new VertexSelectInterface(), new VertexFilterInterface(), new VertexPartitionInterface(), new VertexCompareInterface(), "VertexId");
        EdgeEntity e = new EdgeEntity(input, null, null, null, new EdgePartitionInterface(), new EdgeCompareInterface(), "EdgeId");
        //v.addChild(e);

        JavaRDD rdd = e.runHIPO();
//        Iterator<JavaRDD> it = rdds.iterator();
//        int i = 0;
//        while (it.hasNext()) {
//            JavaRDD rdd = it.next();
        String path = "/opt/jayanth/hipoOutput/";
        rdd.saveAsTextFile(path);
//            i++;
//        }
    }

    private static HashMap<Integer, List<Integer>> generateMetaGraph() throws IOException {
        FileReader fr = new FileReader("/home/dreamlab2/adjlist.json");
        BufferedReader br = new BufferedReader(fr);
        String line;
        HashMap<Integer, List<Integer>> metaGraphMap = new HashMap<Integer, List<Integer>>();
        while ((line = br.readLine()) != null) {
            JSONObject json = new JSONObject(line);
            for (int i = 0; i < 100; i++) {
                ArrayList<Integer> intArray = new ArrayList<Integer>();
                String[] array = json.getString(Integer.toString(i)).split(" ");
                for (int j = 0; j < array.length; j++) {
                    intArray.add(Integer.parseInt(array[j]));
                }
                //System.out.println(intArray);
                metaGraphMap.put(new Integer(i), intArray);
            }
        }
        return metaGraphMap;
    }
}

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by dreamlab2 on 3/22/17.
 */
public class GMLMR {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Logger logger = Logger.getLogger(Map.class);
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //logger.info("Map function");
            //System.out.println("map values " + value.get(new Text("id")));
            String[] val = value.toString().split(" ");
            int id = 0;
//            for (int i = 0; i < val.length; i++) {
                if (val[0].startsWith("id")) {
//                    id = Integer.parseInt(val[i].split(" ")[1]);
                    id = Integer.parseInt(val[1]);
                }
//            }
            context.write(new Text(val[0]),new IntWritable(id));
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable v : values) {
                context.write(key, v);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Logger LOG = Logger.getLogger(GMLMR.class);
        LOG.info("Test");
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(2);
        job.waitForCompletion(true);
    }
}

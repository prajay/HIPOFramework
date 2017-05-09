import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by dreamlab2 on 3/22/17.
 */
public class GMLInputFormat extends FileInputFormat<LongWritable, Text> {
    private static final Log LOG = LogFactory.getLog(GMLInputFormat.class);
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //LOG.info("Filter am here");
        //return new GMLRecordReader();
        return new GMLNewRecordReader();
    }

//    @Override
//    protected boolean isSplitable(JobContext context, Path filename) {
//        return false;
//    }

}

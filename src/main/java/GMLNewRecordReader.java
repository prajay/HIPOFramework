import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by dreamlab2 on 5/4/17.
 */
public class GMLNewRecordReader extends RecordReader<LongWritable, Text> {
    private long start;
    private long pos;
    private long end;
//    private Text key;
//    private MapWritable value;
    private LongWritable key;
    private Text value;
    private Logger LOG = Logger.getLogger(GMLNewRecordReader.class);
    private Map<String, Object> graphMap;
    private FSDataInputStream fileIn;
    private SplitLineReader in;
    private int maxLineLength;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration job = taskAttemptContext.getConfiguration();
        this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength", 2147483647);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        this.fileIn = fs.open(file);
        String delimiter = "]";
        this.in = new UncompressedSplitLineReader(this.fileIn, job, delimiter.getBytes(), split.getLength());
        this.pos = this.start;
        graphMap = new HashMap<String, Object>();
        if (this.pos != 0L) {
            readGraphProperties();
        }
        this.fileIn.seek(this.start);

        if(this.start != 0L) {
            this.start += (long)this.in.readLine(new Text(), 0, this.maxBytesToConsume(this.start));
        }

    }

    private int maxBytesToConsume(long pos) {
        return (int)Math.max(Math.min(2147483647L, this.end - pos), (long)this.maxLineLength);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        retVal = this.pos;

        return retVal;
    }

    private void readGraphProperties() throws IOException {
        StreamTokenizer st = createTokenizer(new InputStreamReader(fileIn));
        while (hasNext(st)) {
            // st.nextToken();
            final int type = st.ttype;
            if (notLineBreak(type)) {
                if (type == ']') {
                    return;
                } else {
                    final String key = st.sval;
                    if (GMLTokens.NODE.equals(key)) {
                        return;
                    } else if (GMLTokens.EDGE.equals(key)) {
                        return;
                    } else if (GMLTokens.GRAPH.equals(key)) {
                        checkValid(st, GMLTokens.GRAPH);
                    } else {
                        // IGNORE
                        parseGraphMap(st);
                        //parseValue("ignore", st);
                    }
                }
            }
        }
    }

    private void parseGraph(final StreamTokenizer st) throws IOException {
        //LOG.info("Filter reached parseGraph");
        while (hasNext(st)) {
            // st.nextToken();
            final int type = st.ttype;
            if (notLineBreak(type)) {
                if (type == ']') {
                    return;
                } else {
                    final String key = st.sval;
                    if (GMLTokens.NODE.equals(key)) {
                        this.key = new LongWritable();
                        this.key.set(this.pos);
                        addNode(parseNode(st));
                        return;
                    } else if (GMLTokens.EDGE.equals(key)) {
                        this.key = new LongWritable();
                        this.key.set(this.pos);
                        addEdge(parseEdge(st));
                        return;
                    } else if (GMLTokens.GRAPH.equals(key)) {
                        checkValid(st, GMLTokens.GRAPH);
                    } else {
                        // IGNORE
                        parseGraphMap(st);
                        //parseValue("ignore", st);
                    }
                }
            }
        }
        throw new IOException("Graph not complete");
    }

    private Map<String, Object> parseNode(final StreamTokenizer st) throws IOException {
        return parseElement(st, GMLTokens.NODE);
    }

    private Map<String, Object> parseEdge(final StreamTokenizer st) throws IOException {
        return parseElement(st, GMLTokens.EDGE);
    }

    private Map<String, Object> parseElement(final StreamTokenizer st, final String node) throws IOException {
        checkValid(st, node);
        return parseMap(node, st);
    }

    private void addNode(Map<String, Object> map) throws IOException {
        //LOG.info(map.get("id"));
//        key = new Text("Node");
//        value = new MapWritable();
        this.value = new Text();
        String stringBuilder = new String("Node");
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            stringBuilder = stringBuilder + "," + entry.getKey() + " " + entry.getValue().toString();
//            value.put(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
        for (Map.Entry<String, Object> entry : graphMap.entrySet()) {
            stringBuilder = stringBuilder + "," + entry.getKey() + " " + entry.getValue().toString();
//            value.put(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
        this.value.set(stringBuilder);
    }

    private void addEdge(Map<String, Object> map) throws IOException {
//        key = new Text("Edge");
//        value = new MapWritable();
        this.value = new Text();
        String stringBuilder = new String("Edge");
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            stringBuilder = stringBuilder + "," + entry.getKey() + " " + entry.getValue().toString();
//            value.put(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
        for (Map.Entry<String, Object> entry : graphMap.entrySet()) {
            stringBuilder = stringBuilder + "," + entry.getKey() + " " + entry.getValue().toString();
//            value.put(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
        this.value.set(stringBuilder);
    }

    private void parseGraphMap(StreamTokenizer st) throws IOException {
        LOG.info("Token: " + st.sval);
        final int type = st.ttype;
        if (notLineBreak(type)) {
            final String key = "graph_" + st.sval;
            final Object value = parseValue(key, st);
            graphMap.put(key, value);
            LOG.info(graphMap.toString());
        }
    }

    private Object parseValue(final String key, final StreamTokenizer st) throws IOException {
        while (hasNext(st)) {
            final int type = st.ttype;
            if (notLineBreak(type)) {
                if (type == StreamTokenizer.TT_NUMBER) {
                    //LOG.info("Token: " + st.nval);
                    //System.out.println(st.nval);
                    final Double doubleValue = Double.valueOf(st.nval);
                    if (doubleValue.equals(Double.valueOf(doubleValue.intValue()))) {
                        return doubleValue.intValue();
                    } else if (doubleValue.equals(Double.valueOf(doubleValue.longValue()))) {
                        return doubleValue.longValue();
                    } else {
                        return doubleValue.floatValue();
                    }
                } else {
                    //LOG.info("Token: " + st.sval);
                    //System.out.println(st.sval);
                    if (type == '[') {
                        return parseMap(key, st);
                    } else if (type == '"') {
                        return st.sval;
                    }
                }
            }
        }
        throw new IOException("value not found");
    }

    private Map<String, Object> parseMap(final String node, final StreamTokenizer st) throws IOException {
        final Map<String, Object> map = new HashMap<String, Object>();
        while (hasNext(st)) {
            final int type = st.ttype;
            if (notLineBreak(type)) {
                if (type == ']') {
                    return map;
                } else {
                    final String key = st.sval;
                    final Object value = parseValue(key, st);
                    //System.out.println(key);
                    map.put(key, value);
                }
            }
        }
        throw new IOException(node + " incomplete");
    }

    private StreamTokenizer createTokenizer(InputStreamReader in) {
        BufferedReader br = new BufferedReader(in);
        StreamTokenizer st = new StreamTokenizer(br);
        st.commentChar(GMLTokens.COMMENT_CHAR);
        st.ordinaryChar('[');
        st.ordinaryChar(']');

        final String stringCharacters = "/\\(){}<>!£$%^&*-+=,.?:;@_`|~";
        for (int i = 0; i < stringCharacters.length(); i++) {
            st.wordChars(stringCharacters.charAt(i), stringCharacters.charAt(i));
        }
        return st;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        key = null;
        value = null;

        int newSize = 0;
        Text v = new Text();

        while(this.getFilePosition() <= this.end || this.in.needAdditionalRecordAfterSplit()) {
            newSize = this.in.readLine(v, this.maxLineLength, this.maxBytesToConsume(this.pos));
            this.pos += (long)newSize;

            if(newSize == 0 || newSize < this.maxLineLength) {
                break;
            }

            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long)newSize));
        }

        String val = v.toString() + "]";
        StreamTokenizer st = createTokenizer(new InputStreamReader(new ByteArrayInputStream(val.getBytes())));
        parseGraph(st);

        if(newSize == 0 || this.key == null) {
            this.key = null;
            this.value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.start == this.end?0.0F:Math.min(1.0F, (float)(this.getFilePosition() - this.start) / (float)(this.end - this.start));
    }

    @Override
    public synchronized void close() throws IOException {
        if(this.in != null) {
            this.in.close();
        }
    }

    private void checkValid(final StreamTokenizer st, final String token) throws IOException {
        if (st.nextToken() != '[') {
            throw new IOException(token + " not followed by [");
        }
    }

    private boolean hasNext(final StreamTokenizer st) throws IOException {
        return st.nextToken() != StreamTokenizer.TT_EOF;
    }

    private boolean notLineBreak(final int type) {
        return type != StreamTokenizer.TT_EOL;
    }
}

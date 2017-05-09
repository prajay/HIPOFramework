import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by dreamlab2 on 3/22/17.
 */
public class GMLRecordReader extends RecordReader<Text, MapWritable> {
    private Text key = null;
    private MapWritable value = null;
    private Path file = null;
    private Configuration jc;
    private StreamTokenizer st = null;
    private Reader r;
    private Map<String, Object> graphMap;
    private Logger LOG = Logger.getLogger(GMLRecordReader.class);

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        file = split.getPath();
        jc = taskAttemptContext.getConfiguration();
        FileSystem fs = file.getFileSystem(jc);
        r = new BufferedReader(new InputStreamReader(fs.open(file)));
        st = new StreamTokenizer(r);
        st.commentChar(GMLTokens.COMMENT_CHAR);
        st.ordinaryChar('[');
        st.ordinaryChar(']');

        final String stringCharacters = "/\\(){}<>!£$%^&*-+=,.?:;@_`|~";
        for (int i = 0; i < stringCharacters.length(); i++) {
            st.wordChars(stringCharacters.charAt(i), stringCharacters.charAt(i));
        }
        graphMap = new HashMap<String, Object>();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //LOG.info("Filter'm at nextkeyvalue");
        key = null;
        value = null;
        parseGraph(st);
        if (key != null)
            return true;
        else
            return false;

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public MapWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }

    public void parse(final StreamTokenizer st) throws IOException {
        while (hasNext(st)) {
            int type = st.ttype;
            if (notLineBreak(type)) {
                final String value = st.sval;
                if (GMLTokens.GRAPH.equals(value)) {
                    parseGraph(st);
                    if (!hasNext(st)) {
                        return;
                    }
                }
            }
        }
        throw new IOException("Graph not complete");
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
                        addNode(parseNode(st));
                        return;
                    } else if (GMLTokens.EDGE.equals(key)) {
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

    private void addNode(Map<String, Object> map) throws IOException {
        //LOG.info(map.get("id"));
        key = new Text("Node");
        value = new MapWritable();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            value.put(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
        for (Map.Entry<String, Object> entry : graphMap.entrySet()) {
            value.put(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
    }

    private void addEdge(Map<String, Object> map) throws IOException {
        key = new Text("Edge");
        value = new MapWritable();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            value.put(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
        for (Map.Entry<String, Object> entry : graphMap.entrySet()) {
            value.put(new Text(entry.getKey()), new Text(entry.getValue().toString()));
        }
    }

    private Object parseValue(final String key, final StreamTokenizer st) throws IOException {
        while (hasNext(st)) {
            final int type = st.ttype;
            if (notLineBreak(type)) {
                if (type == StreamTokenizer.TT_NUMBER) {
                    //LOG.info("Token: " + st.nval);
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

    private boolean parseBoolean(final StreamTokenizer st) throws IOException {
        while (hasNext(st)) {
            final int type = st.ttype;
            if (notLineBreak(type)) {
                if (type == StreamTokenizer.TT_NUMBER) {
                    return st.nval == 1.0;
                }
            }
        }
        throw new IOException("boolean not found");
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
                    map.put(key, value);
                }
            }
        }
        throw new IOException(node + " incomplete");
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

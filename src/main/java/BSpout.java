import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class BSpout extends BaseRichSpout {

    //    Constructor
    public BSpout(int N, int seed) {
        this.N = N;
        this.rand = new Random(seed);
        this.B = createMatrix();
    }

    int N;
    int[][] B;
    Random rand;

    //    Keep track of emitted column/row
    int i = 0;
    int j = 0;

    SpoutOutputCollector spoutOutputCollector;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if (i < N && j < N) {
            this.spoutOutputCollector.emit(new Values(B[i][j], i, j));
            incrementIndex();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("value", "row", "column"));
    }

    //  Creates the matrix
    public int[][] createMatrix() {
        int[][] matrix = new int[N][N];
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                matrix[i][j] = rand.nextInt(N);
            }
        }
        return matrix;
    }

    public void incrementIndex() {
        j++;
        if (j >= N) {
            j = 0;
            i++;
        }
    }
}

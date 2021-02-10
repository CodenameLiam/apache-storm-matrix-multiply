import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class ASpout extends BaseRichSpout {
    public ASpout(int N, int seed) {
        this.N = N;
        this.rand = new Random(seed);
        this.A = createMatrix();
    }

    int N;
    int[][] A;
    Random rand;

    int ARow = 0;
    int AColumn = 0;

    SpoutOutputCollector spoutOutputCollector;


    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if (AColumn < N && ARow < N) {
            this.spoutOutputCollector.emit(new Values(A[ARow][AColumn], ARow));
            incrementIndex();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("AValue", "ARow"));
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
        AColumn++;
        if (AColumn >= N) {
            AColumn = 0;
            ARow++;
        }
    }
}

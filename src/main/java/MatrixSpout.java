import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class MatrixSpout extends BaseRichSpout {
    public MatrixSpout(int N) {
        this.N = N;
        this.A = createMatrix();
        this.B = createMatrix();
    }

    int N;
    int[][] A;
    int[][] B;
    Random rand = new Random(42);

    int ARowIndex = 0;
    int BColumnIndex = 0;

    int operations = 0;


    SpoutOutputCollector spoutOutputCollector;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {

        if (ARowIndex < N) {
            int[] BColumn = getColumn(B, BColumnIndex);
            this.spoutOutputCollector.emit(new Values(A[ARowIndex], BColumn, ARowIndex, BColumnIndex));
            incrementIndex();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ARow", "BColumn", "Ai", "Bj"));
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

    public static int[] getColumn(int[][] array, int index) {
        int[] column = new int[array[0].length]; // Here I assume a rectangular 2D array!
        for (int i = 0; i < column.length; i++) {
            column[i] = array[i][index];
        }
        return column;
    }

    public void incrementIndex() {
        BColumnIndex++;
        if (BColumnIndex >= N) {
            BColumnIndex = 0;
            ARowIndex++;
//            if (ARowIndex >= N) {
//                ARowIndex = 0;
//                operations++;
////                System.out.printf("NUMBER OF OPERATIONS: %d\n\n\n\n", operations);
////                System.out.println("Total execution time: " + System.currentTimeMillis());
//            }
        }
    }
}

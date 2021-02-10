import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class IntegerSpout extends BaseRichSpout {

    SpoutOutputCollector spoutOutputCollector;
    Random rand = new Random(42);

    //  Specifies the number of rows in the matrix
    private Integer N = 10;

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

    //    Gets the row of matrix for a given index
    public static int[] getColumn(int[][] array, int index) {
        int[] column = new int[array[0].length];
        for (int i = 0; i < column.length; i++) {
            column[i] = array[i][index];
        }
        return column;
    }

    //    Assigns the matrix to global variables
    int[][] A = createMatrix();
    int[][] B = createMatrix();

    //    Keeps track of which row/column pair we are multiplying
//    private Integer column = 0;
    private Integer row = 0;


    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple() {
//        if (index < 100) {
//            this.spoutOutputCollector.emit(new Values(index));
//            index++;
//        }
        if (row < N) {
            for (int column = 0; column < N; column++) {
                int[] BCol = getColumn(B, row);
                this.spoutOutputCollector.emit(new Values(A[column], BCol, column, row));
                column++;
            }
            row++;
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("A", "B", "column", "row"));
    }
}

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MultiplierBolt extends BaseBasicBolt {
    //  Specifies the number of rows in the matrix
    private Integer N = 10;
    int[][] C = new int[N][N];

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

//        for (int k = 0; k < N; k++) {
//            C[i][j] = C[i][j] + A[i][k] * B[k][j];
//        }

        int column = (int) tuple.getValueByField("column");
        int row = (int) tuple.getValueByField("row");
        C[row][column] = 0;


        Integer number = tuple.getInteger(0);
        number *= 2;
        basicOutputCollector.emit(new Values(number));
        System.out.println(number);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field"));
    }
}

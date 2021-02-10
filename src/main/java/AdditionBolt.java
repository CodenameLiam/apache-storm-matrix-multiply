import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AdditionBolt extends BaseBasicBolt {
    public AdditionBolt(int N) {
        this.C = new int[N][N];
    }

    int[][] C;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        int result = (int) tuple.getValueByField("result");
        int row = (int) tuple.getValueByField("row");
        int column = (int) tuple.getValueByField("column");

        C[row][column] += result;
        System.out.printf("C value at (%d, %d) is : %d\n", row, column, result);

//        int CRowIndex = (int) tuple.getValueByField("CRowIndex");
//        int CColumnIndex = (int) tuple.getValueByField("CColumnIndex");
//
//        int multiplyResult = (int) tuple.getValueByField("result");
//
//        CResult += multiplyResult;
//
//        basicOutputCollector.emit(new Values(CResult));
//
//        System.out.printf("C value at (%d, %d) is : %d\n", CRowIndex, CColumnIndex, CResult);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("C"));
    }
}

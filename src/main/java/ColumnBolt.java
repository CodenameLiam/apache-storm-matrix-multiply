import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ColumnBolt extends BaseBasicBolt {
    public ColumnBolt(int N) {
        this.BColumn = new int[N];
    }

    int[] BColumn;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        int BValue = (int) tuple.getValueByField("BValue");
        int BColumnIndex = (int) tuple.getValueByField("BColumnIndex");
        int BRowIndex = (int) tuple.getValueByField("BRowIndex");

        int[] ARow = (int[]) tuple.getValueByField("ARow");
        int ARowIndex = (int) tuple.getValueByField("ARowIndex");

        BColumn[BRowIndex] = BValue;

        basicOutputCollector.emit(new Values(ARow, ARowIndex, BColumn, BColumnIndex));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ARow", "ARowIndex", "BColumn", "BColumnIndex"));
    }
}

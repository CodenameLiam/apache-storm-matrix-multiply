import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MultiplicationBolt extends BaseBasicBolt {
    public MultiplicationBolt(int N) {
        this.N = N;
    }

    int N;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        int[] A = (int[]) tuple.getValueByField("ARow");
        int[] B = (int[]) tuple.getValueByField("BColumn");
        int Ci = (int) tuple.getValueByField("Ai");
        int Cj = (int) tuple.getValueByField("Bj");

        int C = 0;

        for (int i = 0; i < N; i++) {
            C += A[i] * B[i];
        }


        System.out.printf("C value at (%d, %d) is : %d\n", Ci, Cj, C);
        basicOutputCollector.emit(new Values(C, Ci, Cj));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result", "row", "column"));
    }
}

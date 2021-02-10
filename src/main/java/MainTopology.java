import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Timer;

public class MainTopology {
    public static void main(String[] args) {
        int N = 4;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Matrix", new MatrixSpout(N));
        builder.setBolt("Multiply", new MultiplicationBolt(N), 24).shuffleGrouping("Matrix");

        Config config = new Config();
//        config.setDebug(true);

        LocalCluster cluster = null;
        try {
            cluster = new LocalCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            cluster.submitTopology("HelloTopology", config, builder.createTopology());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
//                e.printStackTrace();
            }
        } catch (TException e) {
//            e.printStackTrace();
        } finally {
            cluster.shutdown();
        }

    }
}

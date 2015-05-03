package task1.job1; /**
 * Created by guichi on 9/04/2015.
 */
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class JoinPartitioner extends Partitioner<PlaceOrderPair,Text> {

    @Override
    public int getPartition(PlaceOrderPair key, Text value, int numPartition) {
        // TODO Auto-generated method stub
        return (key.getPlace().hashCode() & Integer.MAX_VALUE) % numPartition;
    }

}


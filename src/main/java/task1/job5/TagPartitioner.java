package task1.job5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by guichi on 16/04/2015.
 */
public class TagPartitioner extends Partitioner<Text,IntWritable>{
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        return intWritable.get();
    }
}

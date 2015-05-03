package task1.job4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by guichi on 16/04/2015.
 */
public class MaxPartitioner extends Partitioner<IntWritable,Text>{
    @Override
    public int getPartition(IntWritable intWritable, Text text, int i) {

        return intWritable.get();
    }
}

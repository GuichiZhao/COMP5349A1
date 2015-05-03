package task1.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by guichi on 11/04/2015.
 */
public class SortPartitioner extends Partitioner<IntWritable,Text>{

    @Override
    public int getPartition(IntWritable key, Text value, int num) {
        int k=key.get();
        if (k>10000)
        {
            return 0;
        }
        else if (k>1000)
        {
            return 1;
        }
        else if (k>100)
        {
            return 2;
        }
        else if (k>10)
        {
            return 3;
        }
        else
        {
            return 4;
        }



    }
}

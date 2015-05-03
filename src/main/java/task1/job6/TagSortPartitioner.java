package task1.job6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by guichi on 18/04/2015.
 */
public class TagSortPartitioner extends Partitioner<IntWritable,Text>{
    @Override
    public int getPartition(IntWritable intWritable, Text text, int i) {
        String line=text.toString();
        String[] parts=line.split("\\$");
//
//        for (String s:parts)
//        {
//            System.out.println("\n\n\n\n\n\n\n\n\n\n Part: "+s);
//        }


        return Integer.parseInt(parts[0]);
    }
}

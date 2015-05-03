package task1.job6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 16/04/2015.
 */
public class TagSortMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line =value.toString();
        String [] parts=line.split("\t");
        int count=Integer.parseInt(parts[1]);
        context.write(new IntWritable(count),new Text(parts[0]));

    }
}

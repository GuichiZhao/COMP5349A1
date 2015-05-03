package task1.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 11/04/2015.
 */
public class SortMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String [] parts=line.split("\t");
        if (parts.length==3)
        {
            int count=Integer.parseInt(parts[0]);
            context.write(new IntWritable(count),new Text(parts[1]+"\t"+parts[2]));

            //
        }
    }
}

package task1.job5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 16/04/2015.
 */
public class TagMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String []parts=line.split("\t");
        int rank=Integer.parseInt(parts[0]);
        context.write(new Text(parts[0]+"$"+parts[1]+"$"+parts[2]+"$"+parts[3]),new IntWritable(rank));
    }
}

package task2.job5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 20/04/2015.
 */
public class GroupLocalityMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] parts=line.split("\t");
        context.write(new Text(parts[1]),new Text(parts[0]+"\t"+parts[2]+"\t"+parts[3]));
    }
}

package task2.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 20/04/2015.
 */
public class PhotoFilterMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String [] parts=line.split("\t");
        if (parts.length==6)
        {
            context.write(new Text(parts[1]+"\t"+parts[4]),new Text());
        }
    }
}

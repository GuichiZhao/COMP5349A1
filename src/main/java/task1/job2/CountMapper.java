package task1.job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 10/04/2015.
 */
public class CountMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String [] parts=line.split("\t");
        if (!parts[1].equals("NULL"))
        {
            context.write(new Text(parts[1]),new Text(parts[0]));
        }
    }
}

package task2.job4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 20/04/2015.
 */
public class GroupNeighbourMapper extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line =value.toString();
        String [] parts=line.split("\t");
        String neighbourhood=parts[2];

        if (parts.length==3)
        {

            context.write(new Text(neighbourhood),new Text(parts[0]+"\t"+parts[1]));

        }


    }
}

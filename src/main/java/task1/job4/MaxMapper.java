package task1.job4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by guichi on 11/04/2015.
 */
public class MaxMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
    private static int count=0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] parts=line.split("\t");

        if (count<50)
        {
            count++;
            //StringBuffer stringBuffer=new StringBuffer(parts[2]);
            StringTokenizer stringTokenizer=new StringTokenizer(parts[2]);
            while (stringTokenizer.hasMoreTokens()) {
                context.write(new IntWritable(count), new Text(parts[0] + "\t" + parts[1]+"\t"+stringTokenizer.nextToken()));
            }
        }

    }
}

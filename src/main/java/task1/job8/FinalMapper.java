package task1.job8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 18/04/2015.
 */
public class FinalMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line =value.toString();
        String [] parts=line.split("\t");
        String []lineParts=parts[1].split("\\$");

        int rank=Integer.parseInt(lineParts[0]);
        String tagcount=parts[0];
        String countryCount=lineParts[1];
        String url=lineParts[2];
        String tag=lineParts[3];


        context.write(new IntWritable(rank),new Text(url+"\t"+countryCount+"\t"+tag+"\t"+tagcount+"\t"));

    }
}

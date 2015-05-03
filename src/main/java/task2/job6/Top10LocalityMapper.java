package task2.job6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 22/04/2015.
 */
public class Top10LocalityMapper extends Mapper<LongWritable,Text,CountryLocalityCountPair,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line =value.toString();
        String[] parts=line.split("\t");

        int localityCount=Integer.parseInt(parts[2]);
        context.write(new CountryLocalityCountPair(parts[0],localityCount),new Text(parts[1]+"\t"+parts[3]+"\t"+parts[4]));





    }
}

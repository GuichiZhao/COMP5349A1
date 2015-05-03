package task2.job3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 20/04/2015.
 */
public class PhotoJoinMapper extends Mapper<LongWritable,Text,PlaceOrderPair,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] parts=line.split("\t");

        if (parts.length==2)
        {
            String place_id=parts[1];
            String user_id=parts[0];
            context.write(new PlaceOrderPair(place_id,1),new Text(user_id));
        }

    }
}

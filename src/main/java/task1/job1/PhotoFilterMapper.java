package task1.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Created by guichi on 9/04/2015.
 */
public class PhotoFilterMapper extends Mapper<LongWritable,Text,PlaceOrderPair,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] parts=line.split("\t");
            if (parts.length==6)
            {
                String tags=parts[2];
                String place_id=parts[4];
                context.write(new PlaceOrderPair(place_id,1),new Text(tags));
            }
    }
}

package task2.job3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 20/04/2015.
 */
public class PlaceJoinMapper extends Mapper<LongWritable,Text,PlaceOrderPair,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] parts=line.split("\t");

        if (parts.length == 4) {
            String place_id=parts[0];
            context.write(new PlaceOrderPair(place_id,0),new Text(parts[1]+"\t"+parts[2]+"\t"+parts[3]));
        }

    }


    }


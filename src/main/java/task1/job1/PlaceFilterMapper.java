package task1.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by guichi on 8/04/2015.
 */
public class PlaceFilterMapper extends Mapper<LongWritable,Text,PlaceOrderPair,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String[] parts=line.split("\t");
        String outputKey="";
        // System.out.println(parts.length+"QQQ"+line);
        if (parts.length==7)
        {
            String place_id=parts[0];
            String type_id_String=parts[5];
            String place_url=parts[6];
            int type_id=Integer.parseInt(type_id_String);
            if (type_id==22||type_id==7) {

                if (type_id==7)
                {
                    context.write(new PlaceOrderPair(place_id,0),new Text(place_url));
                }

                else if (type_id==22)
                {


                    String[] url_parts = place_url.split("/");
                    StringBuffer trancated=new StringBuffer();
                    for (int i=0;i<url_parts.length-1;i++)
                    {
                        trancated.append(url_parts[i]);
                        trancated.append("/");
                    }
                    trancated.deleteCharAt(trancated.lastIndexOf("/"));
                    context.write(new PlaceOrderPair(place_id,0),new Text(trancated.toString()));
                }

            }



        }
    }
}

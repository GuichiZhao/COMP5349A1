package task2.job3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by guichi on 20/04/2015.
 */
public class JoinReducer extends Reducer<PlaceOrderPair,Text,Text,Text>{
    @Override
    protected void reduce(PlaceOrderPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator=values.iterator();
        if (key.getOrder().get()==0)
        {
            String placeNmae=iterator.next().toString();
            String[] placeNameParts=placeNmae.split("\t");
            String country=placeNameParts[0];
            String locality=placeNameParts[1];
            String neighbour=placeNameParts[2];
            while (iterator.hasNext())
            {
                iterator.next();
                context.write(new Text(country),new Text(locality+"\t"+neighbour));
            }
        }
    }
}

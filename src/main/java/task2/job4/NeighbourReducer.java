package task2.job4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by guichi on 20/04/2015.
 */
public class NeighbourReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator=values.iterator();
        if (key.toString().equals("NULL"))
        {
            while (iterator.hasNext())
            {
                context.write(iterator.next(),new Text(key+"\t"+"1"));
            }
        }

        //count unique users in a neighbourhood group
        else if (!key.toString().equals("NULL"))
        {
            String countryAndLocality=iterator.next().toString();


            //start from 1,because the cusor has been moved
            int count=1;
            while (iterator.hasNext())
            {
                iterator.next();
                count++;
            }

            context.write(new Text(countryAndLocality),new Text(key+"\t"+count));


        }
    }
}

package task2.job5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by guichi on 20/04/2015.
 */
public class CountAndMaxReducer extends Reducer<Text,Text,Text,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator=values.iterator();
        int localityCount=0;
        String maxNeighbourName="";
        int maxNeighbourCount=0;
        String country="";
        while (iterator.hasNext())
        {
            //context.write(key,iterator.next());
            String info=iterator.next().toString();
            String[] parts=info.split("\t");
            country=parts[0];
            String neighbour=parts[1];
            int neighbourCount=Integer.parseInt(parts[2]);
            localityCount+=neighbourCount;

            if (neighbourCount>maxNeighbourCount)
            {
                maxNeighbourCount=neighbourCount;
                maxNeighbourName=neighbour;
            }
        }

        context.write(new Text(country),new Text(key+"\t"+localityCount+"\t"+maxNeighbourName+"\t"+maxNeighbourCount));
    }
}

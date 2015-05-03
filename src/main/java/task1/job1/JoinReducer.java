package task1.job1; /**
 * Created by guichi on 9/04/2015.
 */



import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class JoinReducer extends  Reducer<PlaceOrderPair, Text, Text, Text> {

    public void reduce(PlaceOrderPair key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        //check if the key is coming from the place table
        Iterator<Text> valuesItr = values.iterator();
        if (key.getOrder().get() == 0){// the key is from the place table
            String placeName = valuesItr.next().toString();
            while (valuesItr.hasNext()){
                //Text value = new Text(valuesItr.next().toString() + "\t" + placeName);
                //context.write(key.getPlace(), value);
                context.write(new Text(valuesItr.next().toString()),new Text(placeName));
            }
        }else{ // the key is not from the place table, but the photo table
            while(valuesItr.hasNext()){
                //context.write(key.getPlace(), new Text(valuesItr.next().toString() + "\t" + "NULL"));
                //some levels of place type is filtered,so can not match a name
                context.write(new Text(valuesItr.next().toString()),new Text("NULL"));
            }
        }
    }
}


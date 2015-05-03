package task1.job5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by guichi on 16/04/2015.
 */
public class TagReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int count=0;
        Iterator itr=values.iterator();
        while (itr.hasNext())
        {
            count++;
            itr.next();

        }

        context.write(key,new IntWritable(count));
    }
}

package task1.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by guichi on 11/04/2015.
 */
public class CountReducer extends Reducer<Text,Text,IntWritable,Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> itr=values.iterator();
        int count=0;
        StringBuffer tagGroup=new StringBuffer();
        while (itr.hasNext())
        {
            tagGroup.append(itr.next());
            tagGroup.append(" ");
            count++;
        }

        context.write(new IntWritable(count),new Text(key.toString()+"\t"+tagGroup));

    }
}

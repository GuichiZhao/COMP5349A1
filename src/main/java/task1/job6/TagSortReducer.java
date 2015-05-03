package task1.job6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by guichi on 18/04/2015.
 */
public class TagSortReducer extends Reducer<IntWritable,Text,Text,Text>{
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    }
}

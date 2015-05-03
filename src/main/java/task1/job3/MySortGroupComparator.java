package task1.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by guichi on 11/04/2015.
 */
public class MySortGroupComparator extends WritableComparator{


    public MySortGroupComparator() {
        super(IntWritable.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable i1=(IntWritable) a;
        IntWritable i2=(IntWritable) b;
        return -1*(i1.compareTo(i2));
    }
}

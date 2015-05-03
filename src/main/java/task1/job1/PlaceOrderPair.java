package task1.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by guichi on 9/04/2015.
 */
public class PlaceOrderPair implements WritableComparable<PlaceOrderPair>{
    private Text place;
    private IntWritable order;

    public IntWritable getOrder() {
        return order;
    }

    public void setOrder(IntWritable order) {
        this.order = order;
    }

    public Text getPlace() {
        return place;
    }

    public void setPlace(Text place) {
        this.place = place;
    }

    public PlaceOrderPair()
    {
        place=new Text();
        order=new IntWritable();
    }

    @Override
    public String toString() {
        return "job1.PlaceOrderPair{" +
                "place=" + place +
                ", order=" + order +
                '}';
    }

    public PlaceOrderPair(String p,int o)
    {
        place=new Text(p);
        order=new IntWritable(o);
    }

    @Override
    public int compareTo(PlaceOrderPair o) {
        int cmp = place.compareTo(o.place);
        if (cmp != 0) {
            return cmp;
        }
        return order.compareTo(o.order);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        place.write(out);
        order.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        place.readFields(in);
        order.readFields(in);
    }
}

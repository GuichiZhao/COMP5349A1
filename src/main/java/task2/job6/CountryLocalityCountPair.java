package task2.job6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by guichi on 22/04/2015.
 */
public class CountryLocalityCountPair  implements WritableComparable<CountryLocalityCountPair>{
    private Text country;
    private IntWritable localityCount;



    public CountryLocalityCountPair()
    {
        country=new Text();
        localityCount=new IntWritable();
    }


    public Text getCountry() {
        return country;
    }

    public CountryLocalityCountPair(String p,int o)
    {
        country=new Text(p);
        localityCount=new IntWritable(o);
    }

    public IntWritable getLocalityCount() {
        return localityCount;
    }

    @Override
    public int compareTo(CountryLocalityCountPair o) {
        int cmp = country.compareTo(o.country);
        if (cmp != 0) {
            return cmp;

        }
        return -1*(localityCount.compareTo(o.localityCount));
    }


    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        country.write(out);
        localityCount.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        country.readFields(in);
        localityCount.readFields(in);
    }

    @Override
    public int hashCode() {
        return country.hashCode();
    }

    @Override
    public String toString() {
        return country+"\t"+localityCount;
    }
}

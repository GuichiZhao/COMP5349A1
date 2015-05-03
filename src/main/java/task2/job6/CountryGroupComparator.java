package task2.job6;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by guichi on 22/04/2015.
 */
public class CountryGroupComparator extends WritableComparator{
    protected CountryGroupComparator() {
        super(CountryLocalityCountPair .class,true);
        // TODO Auto-generated constructor stub
    }

    /**
     * Only compare the key when grouping reducer input together
     */
    public int compare(WritableComparable w1, WritableComparable w2) {
        CountryLocalityCountPair tip1 = (CountryLocalityCountPair) w1;
        CountryLocalityCountPair tip2 = (CountryLocalityCountPair) w2;
        return tip1.getCountry().compareTo(tip2.getCountry());
    }


}

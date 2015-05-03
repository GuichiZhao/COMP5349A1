package task2.job3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class JoinGroupComparator extends WritableComparator {

	protected JoinGroupComparator() {
		super(PlaceOrderPair .class,true);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Only compare the key when grouping reducer input together
	 */
	public int compare(WritableComparable w1, WritableComparable w2) {
		PlaceOrderPair tip1 = (PlaceOrderPair) w1;
		PlaceOrderPair tip2 = (PlaceOrderPair) w2;
		return tip1.getPlace().compareTo(tip2.getPlace());
	}
	
}

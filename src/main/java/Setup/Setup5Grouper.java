package Setup;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Setup5Grouper extends WritableComparator{
	
	public Setup5Grouper() {
		super(PreferKey.class,true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		PreferKey o1=(PreferKey) a;
		PreferKey o2=(PreferKey) b;
		return o1.getJoinkey().compareTo(o2.getJoinkey());
	}
}

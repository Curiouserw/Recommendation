package Chenxiang.Bean;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReferenceGroupComparator extends WritableComparator {
	public ReferenceGroupComparator() {
		super(ReferenceKey.class,true);
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ReferenceKey u1 = (ReferenceKey) a;
		ReferenceKey u2 = (ReferenceKey) b;
		return u2.getUser().compareTo(u1.getUser());
	}
}

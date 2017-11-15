package Chenxiang.Bean;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TagGroupComparator extends WritableComparator {
	public TagGroupComparator() {
		super(PreferTagKey.class ,true);
	}
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		PreferTagKey u1 = (PreferTagKey) a;
		PreferTagKey u2 = (PreferTagKey) b;
		return u1.getId().compareTo(u2.getId());
	}
}

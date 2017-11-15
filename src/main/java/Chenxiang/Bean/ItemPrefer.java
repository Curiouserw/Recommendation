package Chenxiang.Bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ItemPrefer implements WritableComparable<ItemPrefer> {
	private Text item;
	private DoubleWritable preference;
	public ItemPrefer() {
		item = new Text();
		preference = new DoubleWritable();
	}
	
	public void write(DataOutput out) throws IOException {
		item.write(out);
		preference.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		item.readFields(in);
		preference.readFields(in);
	}
	
	public int compareTo(ItemPrefer o) {
		int cmp = item.compareTo(o.item);
		if (cmp != 0) {
			return cmp;
		}
		return preference.compareTo(o.preference);
	}
	public Text getItem() {
		return item;
	}
	public void setItem(Text item) {
		this.item = new Text(item.toString());
	}
	public DoubleWritable getPreference() {
		return preference;
	}
	public void setPreference(DoubleWritable preference) {
		this.preference = new DoubleWritable(preference.get());
	}
	
	public String toString() {
		return item.toString() + ":" +preference.toString();
	}
}

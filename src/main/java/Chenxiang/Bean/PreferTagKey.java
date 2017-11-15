package Chenxiang.Bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PreferTagKey implements WritableComparable<PreferTagKey> {
	private Text id;
	private IntWritable tag;
	public PreferTagKey() {
		id = new Text();
		tag = new IntWritable();
	}
	
	public void write(DataOutput out) throws IOException {
		id.write(out);
		tag.write(out);
	}

	
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		tag.readFields(in);
	}

	
	public int compareTo(PreferTagKey o) {
		int cmp = id.compareTo(o.id);
		if (cmp != 0) {
			return cmp;
		}
		return tag.compareTo(o.tag);
	}
	public Text getId() {
		return id;
	}
	public void setId(Text id) {
		this.id = new Text(id.toString());
	}
	public IntWritable getTag() {
		return tag;
	}
	public void setTag(IntWritable tag) {
		this.tag = new IntWritable(tag.get());
	}
	
	public String toString() {
		return id.toString() + ":" + tag.toString();
	}
}

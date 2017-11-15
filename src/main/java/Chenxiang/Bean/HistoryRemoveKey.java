package Chenxiang.Bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class HistoryRemoveKey implements WritableComparable<HistoryRemoveKey> {
	private Text user;
	private Text item;
	public HistoryRemoveKey() {
		user = new Text();
		item = new Text();
	}

	public void write(DataOutput out) throws IOException {
		user.write(out);
		item.write(out);
	}


	public void readFields(DataInput in) throws IOException {
		user.readFields(in);
		item.readFields(in);
	}


	public int compareTo(HistoryRemoveKey o) {
		int cmp = user.compareTo(o.user);
		if (cmp != 0) {
			return cmp;
		}
		return item.compareTo(o.item);
	}
	public Text getUser() {
		return user;
	}
	public void setUser(Text user) {
		this.user = new Text(user.toString());
	}
	public Text getItem() {
		return item;
	}
	public void setItem(Text item) {
		this.item = new Text(item.toString());
	}
}

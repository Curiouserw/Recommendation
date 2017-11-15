package Chenxiang.Bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UserPrefer implements WritableComparable<UserPrefer> {
	private Text user;
	private DoubleWritable preference;
	public UserPrefer() {
		user = new Text();
		preference = new DoubleWritable();
	}
	
	public void write(DataOutput out) throws IOException {
		user.write(out);
		preference.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		user.readFields(in);
		preference.readFields(in);
	}
	
	public int compareTo(UserPrefer o) {
		int cmp = user.compareTo(o.user);
		if (cmp != 0) {
			return cmp;
		}
		return preference.compareTo(o.preference);
	}
	public Text getUser() {
		return user;
	}
	public void setUser(Text user) {
		this.user = new Text(user.toString());
	}
	
	public String toString() {
		return user + ":" + preference;
	}
	public DoubleWritable getPreference() {
		return preference;
	}
	public void setPreference(DoubleWritable preference) {
		this.preference = new DoubleWritable(preference.get());
	}
}

package Wyl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IdPreference implements WritableComparable<IdPreference> {
	private Text id;  //userid   itemid
	private DoubleWritable preference;
	
	public IdPreference() {
		id = new Text();
		preference = new DoubleWritable();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(id.toString());
		out.writeDouble(preference.get());
	}

	public void readFields(DataInput in) throws IOException {
		id=new Text(in.readUTF());
		preference=new DoubleWritable(in.readDouble());
		
	}

	public int compareTo(IdPreference o) {
		int cmp = id.compareTo(o.id);
		if (cmp != 0) {
			return cmp;
		}
		return preference.compareTo(o.preference);
	}
	public Text getId() {
		return id;
	}
	public void setId(Text id) {
		this.id = new Text(id.toString());
	}
	public DoubleWritable getPreference() {
		return preference;
	}
	public void setPreference(DoubleWritable preference) {
		this.preference = new DoubleWritable(preference.get());
	}
	@Override
	public String toString() {
		return id.toString() + ":" + preference.toString();
	}
}

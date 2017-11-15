package Chenxiang.Bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class ReferenceKey implements WritableComparable<ReferenceKey> ,DBWritable{
	private Text user;
	private Text item;
	private DoubleWritable reference;
	public ReferenceKey() {
		user = new Text();
		item = new Text();
		reference = new DoubleWritable();
	}
	
	public void write(DataOutput out) throws IOException {
		user.write(out);
		item.write(out);
		reference.write(out);
	}

	
	public void readFields(DataInput in) throws IOException {
		user.readFields(in);
		item.readFields(in);
		reference.readFields(in);
	}

	
	public int compareTo(ReferenceKey o) {
		int cmp = user.compareTo(o.user);
		if (cmp != 0) {
			return cmp;
		}
		return o.reference.compareTo(reference);
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

	public DoubleWritable getReference() {
		return reference;
	}

	public void setReference(DoubleWritable reference) {
		this.reference = new DoubleWritable(reference.get());
	}
	
	public String toString() {
		return user.toString() + "\t" + item.toString() + "\t" + reference.toString();
	}
	
	public void readFields(ResultSet rs) throws SQLException {
		if (rs == null) {
			return ;
		}
		user = new Text(rs.getString(1));
		item = new Text(rs.getString(2));
		reference = new DoubleWritable(rs.getDouble(3));
	}
	
	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, user.toString());
		ps.setString(2, item.toString());
		ps.setDouble(3, reference.get());
	}
}

package Wyl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class PreferKey implements WritableComparable<PreferKey>{
	private Long joinkey;
	private Integer flag;
	

	public void write(DataOutput out) throws IOException {
		out.writeLong(this.joinkey);
		out.writeInt(this.flag);
	}


	public void readFields(DataInput in) throws IOException {
		this.joinkey=in.readLong();
		this.flag=in.readInt();
	}


	public int compareTo(PreferKey o) {
		int cmp = joinkey.compareTo(o.joinkey);
		if (cmp != 0)
			return cmp;
		return flag.compareTo(o.flag);
	}

	public String toString() {
		return joinkey + "：" + flag;
//		return "PreferKey [joinkey=" + joinkey + ", flag=" + flag + "]";
	}
	
	public Long getJoinkey() {
		return joinkey;
	}

	public void setJoinkey(Long joinkey) {
		this.joinkey = joinkey;
	}

	public Integer getFlag() {
		return flag;
	}

	public void setFlag(Integer flag) {
		this.flag = flag;
	}

}

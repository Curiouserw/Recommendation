package Setup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Setup5MapperParser  implements Writable{
	private Vector<IdPreference> list ;
	public Setup5MapperParser() {
		list = new Vector<IdPreference>();
	}

	public void parser(String line){
		list.clear();
		String[] split = line.substring(1,line.length()-1).split(",");
		for (String string : split) {
			subPearser(string);
		}
	}
	
	private void subPearser(String subline){
		String[] split = subline.split(":");
		
		IdPreference preference = new IdPreference();
		
		preference.setId(new Text(split[0].trim()));
		preference.setPreference(new DoubleWritable(Double.parseDouble(split[1].trim())));
		
		list.add(preference);
	}


	public void write(DataOutput out) throws IOException {
		IntWritable len = new IntWritable(list.size());
		len.write(out);
		for (IdPreference idPreference : list) {
			idPreference.write(out);
		}
		
	}

	public void readFields(DataInput in) throws IOException {
		list.clear();
		IntWritable len = new IntWritable();
		len.readFields(in);
		for(int i=0;i<len.get();i++) {
			IdPreference preference = new IdPreference();
			preference.readFields(in);
			list.add(preference);
		}
	}

	public Vector<IdPreference> getList() {
		return list;
	}

	public void setList(Vector<IdPreference> list) {
		this.list = list;
	}
}

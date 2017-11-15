package Wyl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Setup6parser {
	private LongWritable key;
	private Text value;
	
	
	
	public void parser(Text line){
		String[] split = line.toString().split("\t");
	}
	
	
	public Text getValue() {
		return value;
	}
	public void setValue(Text value) {
		this.value = value;
	}
	public LongWritable getKey() {
		return key;
	}
	public void setKey(LongWritable key) {
		this.key = key;
	}
	
	
	
}

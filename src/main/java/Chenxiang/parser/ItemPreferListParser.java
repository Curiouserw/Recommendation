package Chenxiang.parser;

import org.apache.hadoop.io.Text;

public class ItemPreferListParser {
	private String item1;
	private String item2;
	private double preference;
	
	public boolean parse(String line) {
		String[] splits = line.split("\t");
		if (splits == null || splits.length != 3) {
			return false;
		} else {
			item1 = splits[0].trim();
			item2 = splits[1].trim();
			preference = Double.parseDouble(splits[2].trim());
			return true;
		}
	}
	public boolean parse(Text line) {
		return parse(line.toString());
	}
	public String getItem1() {
		return item1;
	}
	public void setItem1(String item1) {
		this.item1 = item1;
	}
	public String getItem2() {
		return item2;
	}
	public void setItem2(String item2) {
		this.item2 = item2;
	}
	public double getPreference() {
		return preference;
	}
	public void setPreference(double preference) {
		this.preference = preference;
	}
}

package Chenxiang.parser;

import org.apache.hadoop.io.Text;

public class UserItemParser {
	private String user;
	private String item;
	private double score;
	
	public boolean parse(String line) {
		String[] splits = line.split(" ");
		if (splits == null || splits.length != 3) {
			return false;
		} else {
			user = splits[0].trim();
			item = splits[1].trim();
			score = Double.parseDouble(splits[2].trim());
			return true;
		}
	}
	public boolean parse(Text line) {
		return parse(line.toString());
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getItem() {
		return item;
	}
	public void setItem(String item) {
		this.item = item;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
}

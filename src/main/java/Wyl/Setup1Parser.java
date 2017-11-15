package Wyl;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Setup1Parser {
	private Text userId;
	private Text itemId;
	private DoubleWritable prefer;
//	private boolean isvalid;
	
	public Setup1Parser(){
	}
	
	public void parser(Text context){
		String[] split = context.toString().split(" ");
//		if(split!=null&&split.length>=3){
			this.userId=new Text(split[0].trim());
			this.itemId=new Text(split[1].trim());
			this.prefer=new DoubleWritable(Double.parseDouble(split[2].trim()));
//		}
	}

	public Text getUserId() {
		return userId;
	}

	public void setUserId(Text userId) {
		this.userId = userId;
	}

	public Text getItemId() {
		return itemId;
	}

	public void setItemId(Text itemId) {
		this.itemId = itemId;
	}

	public DoubleWritable getPrefer() {
		return prefer;
	}

	public void setPrefer(DoubleWritable prefer) {
		this.prefer = prefer;
	}
	
	
}

package Chenxiang.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * user Item --> user [items] reduce
 * @author ubuntu
 */
public class UserItemReducer extends Reducer<Text, Text, Text, Text> {
	private ArrayList<Text> list = new ArrayList<>();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		list.clear();
		for (Text v : values) {
			list.add(new Text(v.toString()));Text
		}
		for(int i=0; i<list.size(); i++) {
			context.write(list.get(i), list.get(i));
			for(int j=i+1; j<list.size(); j++) {
				context.write(list.get(i), list.get(j));
				context.write(list.get(j), list.get(i));
			}
		}
	}
}

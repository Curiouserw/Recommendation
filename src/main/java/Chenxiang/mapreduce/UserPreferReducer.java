package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Chenxiang.Bean.UserPrefer;

public class UserPreferReducer extends Reducer<Text, UserPrefer, Text, MapWritable> {
//	private PreferList<UserPrefer> list = new PreferList<UserPrefer>();
	private MapWritable map = new MapWritable();
	@Override
	protected void reduce(Text key, Iterable<UserPrefer> values,
			Context context) throws IOException, InterruptedException {
		map.clear();
		for (UserPrefer userRefer : values) {
//			UserPrefer u = new UserPrefer();
//			u.setUser(userRefer.getUser());
//			u.setPreference(userRefer.getPreference());
//			list.addItem(u);
			map.put(new Text(userRefer.getUser()), new DoubleWritable(userRefer.getPreference().get()));
		}
		context.write(key, map);
		
	}
}

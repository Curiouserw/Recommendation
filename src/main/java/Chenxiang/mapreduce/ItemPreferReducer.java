package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Chenxiang.Bean.ItemPrefer;

public class ItemPreferReducer extends Reducer<Text, ItemPrefer, Text, MapWritable> {
	private MapWritable map = new MapWritable();
	@Override
	protected void reduce(Text key, Iterable<ItemPrefer> values,
			Context context)
					throws IOException, InterruptedException {
		map.clear();
		for (ItemPrefer itemprefer : values) {
			map.put(new Text(itemprefer.getItem()), new DoubleWritable(itemprefer.getPreference().get()));
		}
		
//		StringBuffer sb = new StringBuffer();
//		
//		for (Entry<Writable, Writable> e : map.entrySet()) {
//			sb.append(e.getKey()+":"+e.getValue()+",");
//		}
		context.write(key, map);
//		context.write(key, new Text(sb.toString()));
	}
}

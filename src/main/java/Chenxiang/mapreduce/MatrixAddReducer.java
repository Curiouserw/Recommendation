package Chenxiang.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Chenxiang.Bean.ItemPrefer;
//  使用初始文件空格分割方式
public class MatrixAddReducer extends Reducer<Text, ItemPrefer, Text, NullWritable> {
	private Map<String, Double> map = new HashMap<String, Double>();
	private NullWritable v = NullWritable.get();
	@Override
	protected void reduce(Text key, Iterable<ItemPrefer> values,
			Context context) throws IOException, InterruptedException {
		map.clear();
		for (ItemPrefer value : values) {
			if (map.get(value.getItem().toString())!=null) {
				map.put(value.getItem().toString(), 
						map.get(value.getItem().toString())+value.getPreference().get());
			} else {
				map.put(value.getItem().toString(), 
						value.getPreference().get());
			}
		}
		for(Entry<String, Double> entry:map.entrySet()) {
			context.write(new Text(key.toString() 
					+ " " + entry.getKey().toString() 
					+ " " + entry.getValue().toString()),v);
		}
	}
	
}

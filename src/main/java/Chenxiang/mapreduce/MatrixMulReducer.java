package Chenxiang.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import Chenxiang.Bean.ItemPrefer;
import Chenxiang.Bean.PreferTagKey;

public class MatrixMulReducer extends Reducer<PreferTagKey, MapWritable, Text, ItemPrefer> {
	private Text k = new Text();
	private ItemPrefer v = new ItemPrefer();
	private Map<String, Double> map = new HashMap<String, Double>();
	@Override
	protected void reduce(PreferTagKey key, Iterable<MapWritable> values,
			Context context)
					throws IOException, InterruptedException {
		Iterator<MapWritable> iter = values.iterator();
		MapWritable userlist = iter.next();
		map.clear();
		for (Entry<Writable, Writable> entry:userlist.entrySet()) {
			map.put(entry.getKey().toString(), Double.parseDouble(entry.getValue().toString()));
		}
		while(iter.hasNext()) {
			MapWritable itemlist = iter.next();
			for (Entry<String, Double> entry:map.entrySet()) {
				k.set(entry.getKey());
				for (Entry<Writable, Writable> e:itemlist.entrySet()) {
					v.setItem(new Text(e.getKey().toString()));
					v.setPreference(new DoubleWritable(
							entry.getValue()*Double.parseDouble(e.getValue().toString())));
					context.write(k, v);
					
				}
			}
		}
	}
}

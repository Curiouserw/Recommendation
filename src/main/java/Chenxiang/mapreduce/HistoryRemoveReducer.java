package Chenxiang.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import Chenxiang.Bean.HistoryRemoveKey;

public class HistoryRemoveReducer extends Reducer<HistoryRemoveKey, DoubleWritable, Text, NullWritable> {
	private Text k=new Text();
	private NullWritable v= NullWritable.get();
	@Override
	protected void reduce(HistoryRemoveKey key, Iterable<DoubleWritable> values,
			Context context)
					throws IOException, InterruptedException {
		Iterator<DoubleWritable> iter = values.iterator();
		DoubleWritable first = iter.next();
		if (!iter.hasNext()) {  // no second
			k.set(key.getUser()+" "+key.getItem()+" "+first.get());
			context.write(k, v);
		}
	}
}

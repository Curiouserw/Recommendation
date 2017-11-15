package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import Chenxiang.Bean.ReferenceKey;

public class ReferenceReducer extends Reducer<ReferenceKey, NullWritable, ReferenceKey, NullWritable> {
	@Override
	protected void reduce(ReferenceKey key, Iterable<NullWritable> value,
			Context context)
					throws IOException, InterruptedException {
		int num = Integer.parseInt(context.getConfiguration().get("preferenceNum"));
//		int num = 2;
		for (NullWritable nullWritable : value) {
			if (num > 0) {
				context.write(key, nullWritable);
				num = num -1;
			}
		}
	}
}

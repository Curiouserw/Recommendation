package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import Chenxiang.Bean.PreferTagKey;

public class MatrixItemMulMapper extends Mapper<Text, MapWritable, PreferTagKey, MapWritable>  {
	private PreferTagKey k = new PreferTagKey();
	@Override
	protected void map(Text key, MapWritable value, Context context)
			throws IOException, InterruptedException {
		k.setId(key);
		k.setTag(new IntWritable(1));
		context.write(k, value);
	}
}

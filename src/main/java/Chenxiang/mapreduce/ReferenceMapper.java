package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Chenxiang.Bean.ReferenceKey;
import Chenxiang.parser.UserItemParser;

public class ReferenceMapper extends Mapper<LongWritable, Text, ReferenceKey, NullWritable> {
	private UserItemParser parser = new UserItemParser();
	private ReferenceKey k = new ReferenceKey();
	private NullWritable v = NullWritable.get();
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
					throws IOException, InterruptedException {
		if (parser.parse(value)) {
			k.setUser(new Text(parser.getUser()));
			k.setItem(new Text(parser.getItem()));
			k.setReference(new DoubleWritable(parser.getScore()));
			context.write(k, v);
		}
	}
}

package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Chenxiang.Bean.UserPrefer;
import Chenxiang.parser.UserItemParser;

public class UserPreferMapper extends Mapper<LongWritable, Text, Text, UserPrefer> {
	private UserItemParser parser = new UserItemParser();
	private Text k = new Text();
	private UserPrefer v = new UserPrefer();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (parser.parse(value)) {
			k.set(parser.getItem());
			v.setUser(new Text(parser.getUser()));
			v.setPreference(new DoubleWritable(parser.getScore()));
			context.write(k, v);
		}
	}
}

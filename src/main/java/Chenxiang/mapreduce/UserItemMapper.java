package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Chenxiang.parser.UserItemParser;
/**
 * user Item --> user [items] map
 * @author ubuntu
 */
public class UserItemMapper extends Mapper<LongWritable, Text, Text, Text> {
	private UserItemParser parser = new UserItemParser();
	private Text k = new Text();
	private Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (parser.parse(value)) {
			k.set(parser.getUser());
			v.set(parser.getItem());
			context.write(k, v);
		}
	}
}

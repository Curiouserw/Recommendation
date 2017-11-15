package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Chenxiang.Bean.HistoryRemoveKey;
import Chenxiang.parser.UserItemParser;

public class HistoryRemoveMapper extends Mapper<LongWritable, Text, HistoryRemoveKey, DoubleWritable> {
	private UserItemParser parser = new UserItemParser();
	private HistoryRemoveKey k = new HistoryRemoveKey();
	private DoubleWritable v = new DoubleWritable(0);
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
					throws IOException, InterruptedException {
		if (parser.parse(value)) {
			k.setUser(new Text(parser.getUser()));
			k.setItem(new Text(parser.getItem()));
			context.write(k, v);
		}
	}
}

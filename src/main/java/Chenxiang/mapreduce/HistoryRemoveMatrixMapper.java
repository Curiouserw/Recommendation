package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Chenxiang.Bean.HistoryRemoveKey;
import Chenxiang.parser.UserItemParser;

/*
 * 使用初始文件空格分割方式
 */
public class HistoryRemoveMatrixMapper extends Mapper<LongWritable, Text, HistoryRemoveKey, DoubleWritable> {
	private UserItemParser parser = new UserItemParser();
	private HistoryRemoveKey k = new HistoryRemoveKey();
	private DoubleWritable v = new DoubleWritable();
	@Override
	protected void map(LongWritable key, Text value,
			Context context)
					throws IOException, InterruptedException {
		if (parser.parse(value)) {
			k.setUser(new Text(parser.getUser()));
			k.setItem(new Text(parser.getItem()));
			v.set(parser.getScore());
			context.write(k, v);
		}
	}
}

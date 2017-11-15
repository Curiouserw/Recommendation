package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Chenxiang.Bean.ItemPrefer;
import Chenxiang.parser.ItemPreferListParser;

public class ItemPreferMapper extends Mapper<LongWritable, Text, Text, ItemPrefer> {
	private ItemPreferListParser parser = new ItemPreferListParser();
	private Text k = new Text();
	private ItemPrefer v = new ItemPrefer();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (parser.parse(value)) {
			k.set(parser.getItem1());
			v.setItem(new Text(parser.getItem2()));
			v.setPreference(new DoubleWritable(parser.getPreference()));
			context.write(k, v);
		}
	}
}

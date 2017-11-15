package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Chenxiang.parser.ItemCooreParser;

/**
 * calcul coore 
 * @author ubuntu
 *
 */
public class ItemCooreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private ItemCooreParser parser = new ItemCooreParser();
	private Text k = new Text();
	private IntWritable v = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (parser.parse(value)) {
			k.set(parser.getItem1() + "\t" + parser.getItem2());
			context.write(k, v);
		}
	}
}

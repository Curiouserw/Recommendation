package Chenxiang.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Chenxiang.mapreduce.ItemCooreMapper;

/*
 *  userItemTest finish   cooreCal
 */
public class ItemCooreTest extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		Path input = new Path(conf.get("input"));
		Path output = new Path(conf.get("output"));
		
		Job itemCoorejob = Job.getInstance(conf);
		itemCoorejob.setJobName("Item part");
		itemCoorejob.setJarByClass(this.getClass());
		
		itemCoorejob.setMapperClass(ItemCooreMapper.class);
		itemCoorejob.setMapOutputKeyClass(Text.class);
		itemCoorejob.setMapOutputValueClass(IntWritable.class);
		
		itemCoorejob.setReducerClass(IntSumReducer.class);
		itemCoorejob.setOutputKeyClass(Text.class);
		itemCoorejob.setOutputValueClass(IntWritable.class);
		
		itemCoorejob.setInputFormatClass(TextInputFormat.class);
		itemCoorejob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(itemCoorejob, input);
		FileOutputFormat.setOutputPath(itemCoorejob, output);
		
		itemCoorejob.setCombinerClass(IntSumReducer.class);
//		FileOutputFormat.setCompressOutput(itemCoorejob, true);
//		FileOutputFormat.setOutputCompressorClass(itemCoorejob, BZip2Codec.class);
//		job.setNumReduceTasks(6);
		return itemCoorejob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ItemCooreTest(), args));
	}
	public static ControlledJob getItemCooreJob(Configuration conf) throws Exception {
		Path input = new Path("userItem_ItemCoore");
		Path output = new Path("itemCoore_ItemPreferList");
		
		
		Job itemCoorejob = Job.getInstance(conf);
		itemCoorejob.setJobName("Item part");
		itemCoorejob.setJarByClass(JobControlTest.class);
		
		itemCoorejob.setMapperClass(ItemCooreMapper.class);
		itemCoorejob.setMapOutputKeyClass(Text.class);
		itemCoorejob.setMapOutputValueClass(IntWritable.class);
		
		itemCoorejob.setReducerClass(IntSumReducer.class);
		itemCoorejob.setOutputKeyClass(Text.class);
		itemCoorejob.setOutputValueClass(IntWritable.class);
		
		itemCoorejob.setInputFormatClass(TextInputFormat.class);
		itemCoorejob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(itemCoorejob, input);
		FileOutputFormat.setOutputPath(itemCoorejob, output);
		
		itemCoorejob.setCombinerClass(IntSumReducer.class);
//		FileOutputFormat.setCompressOutput(itemCoorejob, true);
//		FileOutputFormat.setOutputCompressorClass(itemCoorejob, BZip2Codec.class);
//		job.setNumReduceTasks(6);
		
		ControlledJob job = new ControlledJob(itemCoorejob.getConfiguration());
		return job;
	}
}

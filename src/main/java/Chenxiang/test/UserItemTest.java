package Chenxiang.test;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Chenxiang.mapreduce.UserItemMapper;
import Chenxiang.mapreduce.UserItemReducer;
//  1.  coore
public class UserItemTest extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		Path input = new Path(conf.get("input"));
		Path output = new Path(conf.get("output"));
		
		Job useritemjob = Job.getInstance(conf);
		useritemjob.setJobName("UserItem part");
		useritemjob.setJarByClass(this.getClass());
		
		useritemjob.setMapperClass(UserItemMapper.class);
		useritemjob.setMapOutputKeyClass(Text.class);
		useritemjob.setMapOutputValueClass(Text.class);
		
		useritemjob.setReducerClass(UserItemReducer.class);
		useritemjob.setOutputKeyClass(Text.class);
		useritemjob.setOutputValueClass(Text.class);
		
		useritemjob.setInputFormatClass(TextInputFormat.class);
		useritemjob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(useritemjob, input);
		FileOutputFormat.setOutputPath(useritemjob, output);
//		FileOutputFormat.setCompressOutput(useritemjob, true);
//		FileOutputFormat.setOutputCompressorClass(useritemjob, BZip2Codec.class);
//		job.setNumReduceTasks(6);
		return useritemjob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new UserItemTest(), args));
	}
	public static ControlledJob getUserItemJob (Configuration conf) throws Exception {
		
		Path input = new Path(conf.get("input"));
		Path output = new Path("userItem_ItemCoore");
		
		Job useritemjob = Job.getInstance(conf);
		useritemjob.setJobName("UserItem part");
		useritemjob.setJarByClass(JobControlTest.class);
		
		useritemjob.setMapperClass(UserItemMapper.class);
		useritemjob.setMapOutputKeyClass(Text.class);
		useritemjob.setMapOutputValueClass(Text.class);
		
		useritemjob.setReducerClass(UserItemReducer.class);
		useritemjob.setOutputKeyClass(Text.class);
		useritemjob.setOutputValueClass(Text.class);
		
		useritemjob.setInputFormatClass(TextInputFormat.class);
		useritemjob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(useritemjob, input);
		FileOutputFormat.setOutputPath(useritemjob, output);
//		FileOutputFormat.setCompressOutput(useritemjob, true);
//		FileOutputFormat.setOutputCompressorClass(useritemjob, BZip2Codec.class);
//		job.setNumReduceTasks(6);
		ControlledJob job = new ControlledJob(useritemjob.getConfiguration());
		return job;
	}
}

package Chenxiang.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Chenxiang.Bean.UserPrefer;
import Chenxiang.mapreduce.UserPreferMapper;
import Chenxiang.mapreduce.UserPreferReducer;

public class UserPreferListTest extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		Path input = new Path(conf.get("input"));
		Path output = new Path(conf.get("output"));
		
		Job userPreferjob = Job.getInstance(conf);
		userPreferjob.setJobName("userpreferList demo!");
		userPreferjob.setJarByClass(this.getClass());
		
		userPreferjob.setMapperClass(UserPreferMapper.class);
		userPreferjob.setMapOutputKeyClass(Text.class);
		userPreferjob.setMapOutputValueClass(UserPrefer.class);
		
		userPreferjob.setReducerClass(UserPreferReducer.class);
		userPreferjob.setOutputKeyClass(Text.class);
		userPreferjob.setOutputValueClass(MapWritable.class);
		
		userPreferjob.setInputFormatClass(TextInputFormat.class);
		userPreferjob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(userPreferjob, input);
		FileOutputFormat.setOutputPath(userPreferjob, output);
		
//		FileOutputFormat.setCompressOutput(userPreferjob, true);
//		FileOutputFormat.setOutputCompressorClass(userPreferjob, BZip2Codec.class);
//		userPreferjob.setNumReduceTasks(6);
		return userPreferjob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new UserPreferListTest(), args));
	}
	public static ControlledJob getUserPreferJob (Configuration conf) throws Exception {
		
		Path input = new Path(conf.get("input"));
		Path output = new Path("userPrefer_matrixMul");
		
		Job userPreferjob = Job.getInstance(conf);
		userPreferjob.setJobName("userpreferList demo!");
		userPreferjob.setJarByClass(JobControlTest.class);
		
		userPreferjob.setMapperClass(UserPreferMapper.class);
		userPreferjob.setMapOutputKeyClass(Text.class);
		userPreferjob.setMapOutputValueClass(UserPrefer.class);
		
		userPreferjob.setReducerClass(UserPreferReducer.class);
		userPreferjob.setOutputKeyClass(Text.class);
		userPreferjob.setOutputValueClass(MapWritable.class);
		
		userPreferjob.setInputFormatClass(TextInputFormat.class);
		userPreferjob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(userPreferjob, input);
		FileOutputFormat.setOutputPath(userPreferjob, output);
		
//		FileOutputFormat.setCompressOutput(userPreferjob, true);
//		FileOutputFormat.setOutputCompressorClass(userPreferjob, BZip2Codec.class);
//		userPreferjob.setNumReduceTasks(6);
		ControlledJob job = new ControlledJob(userPreferjob.getConfiguration());
		return job;
	}
}

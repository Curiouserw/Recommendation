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

import Chenxiang.Bean.ItemPrefer;
import Chenxiang.mapreduce.ItemPreferMapper;
import Chenxiang.mapreduce.ItemPreferReducer;

public class ItemPreferListTest extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		Path input = new Path(conf.get("input"));
		Path output = new Path(conf.get("output"));
		
		Job itempreferjob = Job.getInstance(conf);
		itempreferjob.setJobName("itempreferList demo!");
		itempreferjob.setJarByClass(this.getClass());
		
		itempreferjob.setMapperClass(ItemPreferMapper.class);
		itempreferjob.setMapOutputKeyClass(Text.class);
		itempreferjob.setMapOutputValueClass(ItemPrefer.class);
		
		itempreferjob.setReducerClass(ItemPreferReducer.class);
		itempreferjob.setOutputKeyClass(Text.class);
		itempreferjob.setOutputValueClass(MapWritable.class);
//		itempreferjob.setOutputValueClass(Text.class);
		
		itempreferjob.setInputFormatClass(TextInputFormat.class);
		itempreferjob.setOutputFormatClass(SequenceFileOutputFormat.class);
//		itempreferjob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(itempreferjob, input);
		FileOutputFormat.setOutputPath(itempreferjob, output);
		
//		FileOutputFormat.setCompressOutput(itempreferjob, true);
//		FileOutputFormat.setOutputCompressorClass(itempreferjob, BZip2Codec.class);
//		itempreferjob.setNumReduceTasks(6);
		return itempreferjob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ItemPreferListTest(), args));
	}
	public static ControlledJob getItemPreferJob(Configuration conf) throws Exception {
		
		Path input = new Path("itemCoore_ItemPreferList");
		Path output = new Path("itemPreferList_matrixMul");
		
		Job itempreferjob = Job.getInstance(conf);
		itempreferjob.setJobName("itempreferList demo!");
		itempreferjob.setJarByClass(JobControlTest.class);
		
		itempreferjob.setMapperClass(ItemPreferMapper.class);
		itempreferjob.setMapOutputKeyClass(Text.class);
		itempreferjob.setMapOutputValueClass(ItemPrefer.class);
		
		itempreferjob.setReducerClass(ItemPreferReducer.class);
		itempreferjob.setOutputKeyClass(Text.class);
		itempreferjob.setOutputValueClass(MapWritable.class);
		
		itempreferjob.setInputFormatClass(TextInputFormat.class);
		itempreferjob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(itempreferjob, input);
		FileOutputFormat.setOutputPath(itempreferjob, output);
		
//		FileOutputFormat.setCompressOutput(itempreferjob, true);
//		FileOutputFormat.setOutputCompressorClass(itempreferjob, BZip2Codec.class);
//		itempreferjob.setNumReduceTasks(6);
		ControlledJob job = new ControlledJob(itempreferjob.getConfiguration());
		return job;
	}
}

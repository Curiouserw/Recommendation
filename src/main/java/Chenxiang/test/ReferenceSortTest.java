package Chenxiang.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Chenxiang.Bean.ReferenceGroupComparator;
import Chenxiang.Bean.ReferenceKey;
import Chenxiang.Bean.ReferencePartitioner;
import Chenxiang.mapreduce.ReferenceMapper;
import Chenxiang.mapreduce.ReferenceReducer;

public class ReferenceSortTest extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		Path input = new Path(conf.get("input"));
		
		Job referencesortjob = Job.getInstance(conf);
		referencesortjob.setJobName("Reference sort part");
		referencesortjob.setJarByClass(this.getClass());
		
		referencesortjob.setMapperClass(ReferenceMapper.class);
		referencesortjob.setMapOutputKeyClass(ReferenceKey.class);
		referencesortjob.setMapOutputValueClass(NullWritable.class);
		
		referencesortjob.setReducerClass(ReferenceReducer.class);
		referencesortjob.setOutputKeyClass(ReferenceKey.class);
		referencesortjob.setOutputValueClass(NullWritable.class);
		
		DBConfiguration.configureDB(
				referencesortjob.getConfiguration(), 			//////
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://172.16.0.100:3306/hadoop", 
				"hadoop", "hadoop");
		
		DBOutputFormat.setOutput(referencesortjob, "husky_preference", "user","item","preference");
		
		
		referencesortjob.setInputFormatClass(TextInputFormat.class);
		referencesortjob.setOutputFormatClass(DBOutputFormat.class);
		
		FileInputFormat.addInputPath(referencesortjob, input);
		
		referencesortjob.setPartitionerClass(ReferencePartitioner.class);
		referencesortjob.setGroupingComparatorClass(ReferenceGroupComparator.class);
		
//		referencesortjob.setNumReduceTasks(6);
		return referencesortjob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new ReferenceSortTest(), args));
	}
	public static ControlledJob getReferenceSortJob(Configuration conf) throws Exception {
		Path input = new Path("history_referencesort");
		
		Job referencesortjob = Job.getInstance(conf);
		referencesortjob.setJobName("Reference sort part");
		referencesortjob.setJarByClass(JobControlTest.class);
		
		referencesortjob.setMapperClass(ReferenceMapper.class);
		referencesortjob.setMapOutputKeyClass(ReferenceKey.class);
		referencesortjob.setMapOutputValueClass(NullWritable.class);
		
		referencesortjob.setReducerClass(ReferenceReducer.class);
		referencesortjob.setOutputKeyClass(ReferenceKey.class);
		referencesortjob.setOutputValueClass(NullWritable.class);
		
		DBConfiguration.configureDB(
				referencesortjob.getConfiguration(), 			//////
				"com.mysql.jdbc.Driver", 
				"jdbc:mysql://172.16.0.100:3306/hadoop", 
				"hadoop", "hadoop");
		
		DBOutputFormat.setOutput(referencesortjob, "husky_preference", "user","item","preference");
		
		
		referencesortjob.setInputFormatClass(TextInputFormat.class);
		referencesortjob.setOutputFormatClass(DBOutputFormat.class);
		
		FileInputFormat.addInputPath(referencesortjob, input);
		
		referencesortjob.setPartitionerClass(ReferencePartitioner.class);
		referencesortjob.setGroupingComparatorClass(ReferenceGroupComparator.class);
		
//		referencesortjob.setNumReduceTasks(6);
		ControlledJob job = new ControlledJob(referencesortjob.getConfiguration());
		return job;
	}
}

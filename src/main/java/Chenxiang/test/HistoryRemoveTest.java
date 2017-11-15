package Chenxiang.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Chenxiang.Bean.HistoryRemoveKey;
import Chenxiang.mapreduce.HistoryRemoveMapper;
import Chenxiang.mapreduce.HistoryRemoveMatrixMapper;
import Chenxiang.mapreduce.HistoryRemoveReducer;

public class HistoryRemoveTest extends Configured implements Tool {


	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		Path firstinput = new Path(conf.get("firstinput"));
		Path secondinput = new Path(conf.get("secondinput"));
		Path output = new Path(conf.get("output"));
		
		Job historyremovejob = Job.getInstance(conf);
		historyremovejob.setJobName("history remove part");
		historyremovejob.setJarByClass(this.getClass());
		
		historyremovejob.setMapOutputKeyClass(HistoryRemoveKey.class);
		historyremovejob.setMapOutputValueClass(DoubleWritable.class);
		
		historyremovejob.setReducerClass(HistoryRemoveReducer.class);
		historyremovejob.setOutputKeyClass(Text.class);
		historyremovejob.setOutputValueClass(NullWritable.class);
		
		MultipleInputs.addInputPath(historyremovejob, firstinput, TextInputFormat.class,HistoryRemoveMapper.class);
		MultipleInputs.addInputPath(historyremovejob, secondinput, TextInputFormat.class,HistoryRemoveMatrixMapper.class);
		historyremovejob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(historyremovejob, output);
		
//		FileOutputFormat.setCompressOutput(historyremovejob, true);
//		FileOutputFormat.setOutputCompressorClass(historyremovejob, BZip2Codec.class);
//		historyremovejob.setNumReduceTasks(6);
		return historyremovejob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new HistoryRemoveTest(), args));
	}
	public static ControlledJob getHistoryRemoveJob(Configuration conf) throws Exception {
		
		Path firstinput = new Path(conf.get("input"));
		Path secondinput = new Path("matrixadd_historyremove");
		Path output = new Path("history_referencesort");
		
		Job historyremovejob = Job.getInstance(conf);
		historyremovejob.setJobName("history remove part");
//		historyremovejob.setJarByClass(this.getClass());
		historyremovejob.setJarByClass(JobControlTest.class);
		
		historyremovejob.setMapOutputKeyClass(HistoryRemoveKey.class);
		historyremovejob.setMapOutputValueClass(DoubleWritable.class);
		
		historyremovejob.setReducerClass(HistoryRemoveReducer.class);
		historyremovejob.setOutputKeyClass(Text.class);
		historyremovejob.setOutputValueClass(NullWritable.class);
		
		MultipleInputs.addInputPath(historyremovejob, firstinput, TextInputFormat.class,HistoryRemoveMapper.class);
		MultipleInputs.addInputPath(historyremovejob, secondinput, TextInputFormat.class,HistoryRemoveMatrixMapper.class);
		historyremovejob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(historyremovejob, output);
		
//		FileOutputFormat.setCompressOutput(historyremovejob, true);
//		FileOutputFormat.setOutputCompressorClass(historyremovejob, BZip2Codec.class);
//		historyremovejob.setNumReduceTasks(6);
		
		ControlledJob job = new ControlledJob(historyremovejob.getConfiguration());
		return job;
	}
}

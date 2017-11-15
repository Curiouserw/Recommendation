package Chenxiang.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Chenxiang.Bean.ItemPrefer;
import Chenxiang.mapreduce.MatrixAddMapper;
import Chenxiang.mapreduce.MatrixAddReducer;

public class MatrixAddListTest extends Configured implements Tool {

	
	public int run(String[] args) throws Exception {

		Configuration conf= getConf();
		Path input = new Path(conf.get("input"));
		Path output = new Path(conf.get("output"));
		
		Job matrixAddjob = Job.getInstance(conf);
		matrixAddjob.setJobName("matrixAddList part");
		matrixAddjob.setJarByClass(this.getClass());
		
		matrixAddjob.setMapperClass(MatrixAddMapper.class);
		matrixAddjob.setMapOutputKeyClass(Text.class);
		matrixAddjob.setMapOutputValueClass(ItemPrefer.class);
		
		matrixAddjob.setReducerClass(MatrixAddReducer.class);
		matrixAddjob.setOutputKeyClass(Text.class);
		matrixAddjob.setOutputValueClass(NullWritable.class);
		
		matrixAddjob.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(matrixAddjob,input);
		matrixAddjob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(matrixAddjob, output);
		
//		FileOutputFormat.setCompressOutput(matrixAddjob, true);
//		FileOutputFormat.setOutputCompressorClass(matrixAddjob, BZip2Codec.class);
//		matrixAddjob.setNumReduceTasks(6);
		return matrixAddjob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MatrixAddListTest(), args));
	}
	public static ControlledJob getMatrixAddJob(Configuration conf) throws Exception {
		
		Path input = new Path("matrixmul_matrixadd");
		Path output = new Path("matrixadd_historyremove");
		
		Job matrixAddjob = Job.getInstance(conf);
		matrixAddjob.setJobName("matrixAddList part");
		matrixAddjob.setJarByClass(JobControlTest.class);
		
		matrixAddjob.setMapperClass(MatrixAddMapper.class);
		matrixAddjob.setMapOutputKeyClass(Text.class);
		matrixAddjob.setMapOutputValueClass(ItemPrefer.class);
		
		matrixAddjob.setReducerClass(MatrixAddReducer.class);
		matrixAddjob.setOutputKeyClass(Text.class);
		matrixAddjob.setOutputValueClass(NullWritable.class);
		
		matrixAddjob.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(matrixAddjob,input);
		matrixAddjob.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(matrixAddjob, output);
		
//		FileOutputFormat.setCompressOutput(matrixAddjob, true);
//		FileOutputFormat.setOutputCompressorClass(matrixAddjob, BZip2Codec.class);
//		matrixAddjob.setNumReduceTasks(6);
		ControlledJob job = new ControlledJob(matrixAddjob.getConfiguration());
		return job;
	}
}

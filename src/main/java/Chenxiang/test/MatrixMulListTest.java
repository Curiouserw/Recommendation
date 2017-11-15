package Chenxiang.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Chenxiang.Bean.ItemPrefer;
import Chenxiang.Bean.PreferTagKey;
import Chenxiang.Bean.TagGroupComparator;
import Chenxiang.mapreduce.MatrixItemMulMapper;
import Chenxiang.mapreduce.MatrixMulReducer;
import Chenxiang.mapreduce.MatrixUserMulMapper;

public class MatrixMulListTest extends Configured implements Tool {
	
	
	public int run(String[] args) throws Exception {
		Configuration conf= getConf();
		Path firstinput = new Path(conf.get("firstinput"));
		Path secondinput = new Path(conf.get("secondinput"));
		Path output = new Path(conf.get("output"));
		
		Job matrixMuljob = Job.getInstance(conf);
		matrixMuljob.setJobName("matrixMulList part");
		matrixMuljob.setJarByClass(this.getClass());
		
		matrixMuljob.setMapOutputKeyClass(PreferTagKey.class);
		matrixMuljob.setMapOutputValueClass(MapWritable.class);
		
		matrixMuljob.setReducerClass(MatrixMulReducer.class);
		matrixMuljob.setOutputKeyClass(Text.class);
		matrixMuljob.setOutputValueClass(ItemPrefer.class);
		
		MultipleInputs.addInputPath(matrixMuljob, firstinput, SequenceFileInputFormat.class,MatrixUserMulMapper.class);
		MultipleInputs.addInputPath(matrixMuljob, secondinput, SequenceFileInputFormat.class,MatrixItemMulMapper.class);
		matrixMuljob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(matrixMuljob, output);
		
		matrixMuljob.setGroupingComparatorClass(TagGroupComparator.class);
		
//		FileOutputFormat.setCompressOutput(matrixMuljob, true);
//		FileOutputFormat.setOutputCompressorClass(matrixMuljob, BZip2Codec.class);
//		job.setNumReduceTasks(6);
		return matrixMuljob.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MatrixMulListTest(), args));
	}
	public static ControlledJob getMatrixMulJob (Configuration conf) throws Exception {
		
		Path firstinput = new Path("userPrefer_matrixMul");
		Path secondinput = new Path("itemPreferList_matrixMul");
		Path output = new Path("matrixmul_matrixadd");
		
		Job matrixMuljob = Job.getInstance(conf);
		matrixMuljob.setJobName("matrixMulList part");
		matrixMuljob.setJarByClass(JobControlTest.class);
		
		matrixMuljob.setMapOutputKeyClass(PreferTagKey.class);
		matrixMuljob.setMapOutputValueClass(MapWritable.class);
		
		matrixMuljob.setReducerClass(MatrixMulReducer.class);
		matrixMuljob.setOutputKeyClass(Text.class);
		matrixMuljob.setOutputValueClass(ItemPrefer.class);
		
		MultipleInputs.addInputPath(matrixMuljob, firstinput, SequenceFileInputFormat.class,MatrixUserMulMapper.class);
		MultipleInputs.addInputPath(matrixMuljob, secondinput, SequenceFileInputFormat.class,MatrixItemMulMapper.class);
		matrixMuljob.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(matrixMuljob, output);
		
		matrixMuljob.setGroupingComparatorClass(TagGroupComparator.class);
		
//		FileOutputFormat.setCompressOutput(matrixMuljob, true);
//		FileOutputFormat.setOutputCompressorClass(matrixMuljob, BZip2Codec.class);
//		job.setNumReduceTasks(6);
		
		ControlledJob job = new ControlledJob(matrixMuljob.getConfiguration());
		return job;
	}
}

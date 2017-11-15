package Wyl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Setup2 extends Configured implements Tool{
	
	private static class Setup2Mapper extends Mapper<LongWritable, Text,Text, IntWritable>{
		IntWritable v=new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			context.write(value, v);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//---------in=out=/user/hdfs/Setup1-Result-4/part-r-00000
		Path in=new Path(conf.get("in"));
		//---------out=out=/user/hdfs/Setup2-Result-1/part-r-00000
		Path out=new Path(conf.get("out"));

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("Recommendation-Setup2");
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, in);
		
		job.setMapperClass(Setup2Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Setup2(), args));
	}
}

package Wyl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Setup6 extends Configured implements Tool{
	private static class Setup6Mapper extends Mapper<LongWritable,Text,LongWritable, Text>{
		LongWritable k=new LongWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			
			
//			k.set(s);
			
			
		}
	}
	private static class Setup6Reducer extends Reducer<LongWritable, Text, LongWritable, Text>{
		@Override
		protected void reduce(LongWritable arg0, Iterable<Text> arg1,Context arg2) throws IOException, InterruptedException {
		
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(this.getClass());
		job.setJobName("RationalMonset-Setup6");
		
		job.setMapperClass(Setup6Mapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(Setup6Reducer.class);
		
		
		return 0;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Setup6(), args));
	}
}

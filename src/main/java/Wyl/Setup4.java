package Wyl;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Setup4 extends Configured implements Tool{
	
	private static class Setup4Mapper extends Mapper<LongWritable, Text,Text, Text>{
		Text k=new Text();
		Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			String[] split = value.toString().split(" ");
			k.set(split[1].trim());
			v.set(split[0]+":"+Double.parseDouble(split[2].trim()));
			context.write(k,v);
		}
	}
	private static class Setup4Reducer extends Reducer<Text, Text, Text, Text>{
		StringBuffer b=new StringBuffer();
		Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value,Context context)throws IOException, InterruptedException {
			b.setLength(0);
			for (Text tx : value) {
				b.append(tx.toString()+", ");
			}
			b.setLength(b.length()-2);
			v.set("["+b.toString().trim()+"]");
			context.write(key,v);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//--------------in=/data/matrix.txt
		Path in=new Path(conf.get("in"));
		//--------------out=/user/hdfs/Setup4-Result-1/part-r-00000
		Path out=new Path(conf.get("out"));

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("Recommendation-Setup4");
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, in);
		
		job.setMapperClass(Setup4Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(Setup4Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Setup4(), args));
	}
}


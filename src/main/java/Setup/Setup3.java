package Setup;

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

public class Setup3 extends Configured implements Tool{
	
	private static class Setup3Mapper extends Mapper<LongWritable, Text,Text, Text>{
		Text k=new Text();
		Text v=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			k.set(split[0].trim());
			v.set(split[1].trim()+":"+Double.parseDouble(split[2].trim()));
			context.write(k, v);
		}
	}
	private static class Setup3Reducer extends Reducer<Text, Text, Text, Text>{
		StringBuffer c=new StringBuffer();
		Text v=new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value,Context context)throws IOException, InterruptedException {
			c.setLength(0);
			for (Text tx : value) {
				c.append(tx.toString()+", ");
			}
			c.setLength(c.length()-2);
			v.set("["+c.toString().trim()+"]");
			context.write(key,v);
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//--------------in=/user/hdfs/Setup2-Result-1/part-r-00000
		Path in=new Path(conf.get("in"));
		//--------------out=/user/hdfs/Setup3-Result-1/part-r-00000
		Path out=new Path(conf.get("out"));

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("Recommendation-Setup3");
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, in);
		
		job.setMapperClass(Setup3Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(Setup3Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Setup3(), args));
	}
}


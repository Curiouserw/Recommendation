package Wyl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Setup1 extends Configured implements Tool {
//Setup1-Mapper	
	public static class Setup1Mapper extends Mapper<LongWritable, Text, Text, Text>{
		Setup1Parser parser=new Setup1Parser();
	
		@Override
		protected void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException {
			parser.parser(value);
			context.write(parser.getUserId(),parser.getItemId());
		}
	}
	
	
	private static class Setup1Reducer extends Reducer<Text, Text, Text, Text>{
		Text k=new Text();
		Text v=new Text();
		List<Text> l=new ArrayList<Text>();
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException {
			l.clear();
			for (Text t : value) {
				l.add(new Text(t));
			}
			for (int i = 0; i < l.size(); i++) {
				k=l.get(i);
				for (int j = 0; j < l.size(); j++) {
					v=l.get(j);
					context.write(k,v);
				}
			}
		}
	}
	
	
	

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//-----------in=/data/rmc/process/input/matrix.txt
		Path in=new Path(conf.get("in"));
		//------------out=/user/hdfs/Setup1-Result-4/part-r-00000
		Path out=new Path(conf.get("out"));
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("Recommendation-Setup1");
		
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job, in);
		
		job.setMapperClass(Setup1Mapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(Setup1Reducer.class);
//		ChainReducer.setReducer(job, Setup1Reducer.class, Text.class, Text.class, Text.class, Text.class, conf);
//		ChainReducer.addMapper(job, Setup12Mapper.class,Text.class, Text.class, Text.class, IntWritable.class,conf);
//		ChainReducer.setReducer(job, IntSumReducer.class, Text.class, IntWritable.class, Text.class, IntWritable.class,conf);
//		ChainReducer.addMapper(job, , inputKeyClass, inputValueClass, outputKeyClass, outputValueClass, mapperConf);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
//		TextOutputFormat.setCompressOutput(job, true);
//		TextOutputFormat.setOutputCompressorClass(job, Bzip2Compressor.class);
		
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Setup1(), args));
	}

}

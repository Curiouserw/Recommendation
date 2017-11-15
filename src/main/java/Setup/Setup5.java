package Setup;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Setup5 extends Configured implements Tool{
	
	private static class Setup5Mapper1 extends Mapper<LongWritable, Text,PreferKey, Setup5MapperParser>{
		PreferKey k=new PreferKey();
		Setup5MapperParser  parser = new Setup5MapperParser();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			k.setJoinkey(Long.parseLong(split[0]));
			k.setFlag(0);
			parser.parser(split[1].trim());
			context.write(k, parser);
		}
	}
	private static class Setup5Mapper2 extends Mapper<LongWritable, Text,PreferKey, Setup5MapperParser>{
		PreferKey k=new PreferKey();
		Setup5MapperParser parser1 = new Setup5MapperParser();
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			k.setJoinkey(Long.parseLong(split[0]));
			k.setFlag(1);
			parser1.parser(split[1].trim());
			context.write(k, parser1);
		}
	}
	private static class Setup5Reducer extends Reducer<PreferKey, Setup5MapperParser, Text, IdPreference>{
//		private Text k = new Text();
//		private MapWritable map = new MapWritable();
		private Vector<IdPreference> list = new Vector<IdPreference>();
		
		private Text k = new Text();
		private IdPreference v = new IdPreference();
		@Override
		protected void reduce(PreferKey key, Iterable<Setup5MapperParser> values, Context context)throws IOException, InterruptedException {
			
//			Iterator<Setup5MapperParser> iterator = values.iterator();
//			Setup5MapperParser next = iterator.next();
			
//			while(iterator.hasNext()) {
//				Vector<IdPreference> itemlist = next.getList();
//				StringBuffer sb = new StringBuffer();
//				for(IdPreference en : itemlist) {
//					sb.append(en.getId() + ":" + en.getPreference() + ",");
//				}
//				context.write(new Text("itemlist"), new Text(sb.toString()));
//				
//				Vector<IdPreference> userlist = iterator.next().getList();
//				sb.setLength(0);
//				for(IdPreference en : userlist) {
//					sb.append(en.getId() + ":" + en.getPreference() + ",");
//				}
//				context.write(new Text("userlist"), new Text(sb.toString()));
//			}
			
//			Vector<IdPreference> userlist = next.getList();
//			for (IdPreference idPreference : userlist) {
//				IdPreference preference = new IdPreference();
//				preference.setId(idPreference.getId());
//				preference.setPreference(idPreference.getPreference());
//				list.add(preference);
//			}
//			Vector<IdPreference> itemlist = iterator.next().getList();
//			
//			for (IdPreference idPreference : list) {
//				k.set(idPreference.getId().toString());
//				for (IdPreference itemprefer : itemlist) {
//					map.put(new Text(itemprefer.getId()), new DoubleWritable(itemprefer.getPreference().get()*idPreference.getPreference().get()));
//				}
////				context.write(k, map);
//				StringBuffer sb = new StringBuffer();
//				for(Entry<Writable, Writable> en : map.entrySet()) {
//					sb.append(en.getKey() + ":" + en.getValue() + ",");
//				}
//				context.write(k, new Text(sb.toString()));
//				map.clear();
//			}
//			list.clear();
			
//			Iterator<Setup5MapperParser> iter = values.iterator();
//			Setup5MapperParser itemlist = iter.next();
//			for (IdPreference idPreference:itemlist.getList()) {
////				IdPreference preference = new IdPreference();
////				preference.setId(idPreference.getId());
////				preference.setPreference(idPreference.getPreference());
//				list.add(idPreference);
//			}
//			while(iter.hasNext()) {
//				Setup5MapperParser userlist = iter.next();
//				for (IdPreference entry:list) {
//					k.set(entry.getId());
//					for (IdPreference e:userlist.getList()) {
//						v.setId(new Text(e.getId().toString()));
//						v.setPreference(new DoubleWritable(
//								entry.getPreference().get()*e.getPreference().get()));
//						context.write(k, v);
//					}
//				}
//			}
			
			
//			Iterator<Setup5MapperParser> it = values.iterator();
//			Setup5MapperParser itemList = it.next();
//			for (IdPreference i : itemList.getList()) {
//				
//			}
			
			Iterator<Setup5MapperParser> it = values.iterator();
			Setup5MapperParser intemlist = it.next();
			while(it.hasNext()){
				Setup5MapperParser userlist = it.next();
			}
			
			
		}
	}

//	static class DemoReducer extends Reducer<PreferKey, Setup5MapperParser, Text, Text>{
//		private Vector<IdPreference> list=new Vector<>();
//
//		@Override
//		protected void reduce(PreferKey key, Iterable<Setup5MapperParser> values,
//				Reducer<PreferKey, Setup5MapperParser, Text, Text>.Context context)
//				throws IOException, InterruptedException {
//			list.clear();
////			for (Setup5MapperParser setup5MapperParser : values) {
////				list = setup5MapperParser.getList();
////				context.write(new Text(key.toString()), new Text(list.toString()));
////			}
//			Iterator<Setup5MapperParser> iterator = values.iterator();
//
//			context.write(new Text(key.getJoinkey().toString()), new Text(iterator.next().getList().toString()+iterator.next().getList().toString()));
//		}
//		
//	}
	
	

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//--------------in3-itemlist=/user/hdfs/Setup4-Result-1/part-r-00000
		Path in3=new Path(conf.get("in3"));
		//-------------in4-userList=/user/hdfs/Setup3-Result-1/part-r-00000
		Path in4=new Path(conf.get("in4"));
		//--------------out=/user/hdfs/Setup5-Result-1/part-r-00000
		Path out=new Path(conf.get("out"));

		Job job = Job.getInstance(conf);
		job.setJarByClass(this.getClass());
		job.setJobName("Recommendation-Setup5");
		
		MultipleInputs.addInputPath(job, in3, TextInputFormat.class, Setup5Mapper1.class);
		MultipleInputs.addInputPath(job, in4, TextInputFormat.class, Setup5Mapper2.class);
		job.setMapOutputKeyClass(PreferKey.class);
		job.setMapOutputValueClass(Setup5MapperParser.class);
		
		job.setReducerClass(Setup5Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, out);
		
		job.setPartitionerClass(Setup5Partitioner.class);
		job.setGroupingComparatorClass(Setup5Grouper.class);
		
		
		return job.waitForCompletion(true)?0:1;
	}
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Setup5(), args));
	}
}


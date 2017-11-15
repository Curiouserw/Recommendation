package Chenxiang.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Chenxiang.Bean.ItemPrefer;

public class MatrixAddMapper extends Mapper<Text, ItemPrefer, Text, ItemPrefer> {
	@Override
	protected void map(Text key, ItemPrefer value, Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}
}

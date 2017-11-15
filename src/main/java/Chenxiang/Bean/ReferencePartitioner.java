package Chenxiang.Bean;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ReferencePartitioner extends Partitioner<ReferenceKey, NullWritable> {

	@Override
	public int getPartition(ReferenceKey key, NullWritable arg1, int num) {
		return Math.abs(key.getUser().hashCode()*163) % num;
	}

}

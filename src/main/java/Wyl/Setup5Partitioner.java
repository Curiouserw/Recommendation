package Wyl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Setup5Partitioner extends Partitioner<PreferKey, Text>{

	@Override
	public int getPartition(PreferKey key, Text value, int numPartitions) {
		return Math.abs(key.getJoinkey().hashCode() * 127) % numPartitions;
	}
}

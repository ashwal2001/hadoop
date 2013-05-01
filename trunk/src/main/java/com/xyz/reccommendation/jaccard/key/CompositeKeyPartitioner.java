package com.xyz.reccommendation.jaccard.key;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CompositeKeyPartitioner extends Partitioner<TextDoublePair, Text> {

	@Override
	public int getPartition(TextDoublePair key, Text val, int numPartitions) {
		int hash = key.getFirst().hashCode();
		int partition = Math.abs(hash) % numPartitions;
		return partition;
	}

}

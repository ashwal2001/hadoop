package com.xyz.reccommendation.grouping;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.xyz.reccommendation.key.RecoEngineCompositeKey;

public class RecoEngineGroupingComparator extends WritableComparator {

	protected RecoEngineGroupingComparator() {
		super(RecoEngineCompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		RecoEngineCompositeKey key1 = (RecoEngineCompositeKey) w1;
		RecoEngineCompositeKey key2 = (RecoEngineCompositeKey) w2;

		// (check on SessionId)
		return key1.getSessionId().compareTo(key2.getSessionId());
	}

}

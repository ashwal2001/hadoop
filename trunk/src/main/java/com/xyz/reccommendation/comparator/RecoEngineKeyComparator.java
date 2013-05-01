package com.xyz.reccommendation.comparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.xyz.reccommendation.key.RecoEngineCompositeKey;

public class RecoEngineKeyComparator extends WritableComparator {
	protected RecoEngineKeyComparator() {
		super(RecoEngineCompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		RecoEngineCompositeKey key1 = (RecoEngineCompositeKey) w1;
		RecoEngineCompositeKey key2 = (RecoEngineCompositeKey) w2;

		// (first check on SessionId)
		int compare = key1.getSessionId().compareTo(key2.getSessionId());

		if (compare == 0) {
			// only if we are in the same input group should we try and sort by
			// value (datetime)
			return key1.getDatetime().compareTo(key2.getDatetime());
			//return key2.getDatetime().compareTo(key1.getDatetime());
		}

		return compare;
	}
}

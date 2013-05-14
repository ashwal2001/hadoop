package com.xyz.reccommendation.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

/**
 * Reduce class that does the final normalization. Takes in data of the form (xi
 * xj (sum of counts)) Outputs the similarity between xi and xj as (the number
 * of y values xi and xj are associated with)/(the sum of the counts)
 */
public class NormalizationReduce extends
		Reducer<Text, Text, Text, Text> {
	/**
	 * @param ikey
	 *            The xi xj pair for which we are calculating the similarity
	 * @param vlist
	 *            The list of ((xi,xj),sum of counts) pairs
	 * @param output
	 *            OutputCollector that collects a (Text, FloatWritable) pair.
	 *            where the Text is the (xi,xj) pair and the FloatWritable is
	 *            their similarity
	 * @param reporter
	 *            Used by Hadoop, but we do not use it
	 */
	@Override
	public void reduce(Text ikey, Iterable<Text> vlist, Context context)
			throws IOException, InterruptedException {
		float count = 0;
		float count1 = 0, count2 = 0;
		float sumcounts = 0;

		for (final Text val : vlist) {
			// System.out.println(val.toString());
			if (!val.toString().isEmpty() && !val.toString().trim().isEmpty()) {
				String[] token = val.toString().split("\\|+");
				count1 = Float.parseFloat(token[0]);
				count2 = Float.parseFloat(token[1]);
				sumcounts = count1 + count2;
			}
			count++;
		}
		String[] keyArray = ikey.toString().split("\\|");

		float sumCosine = (float) (count / Math.sqrt(count1 * count2));
		float sumJaccard = (float) (count / (sumcounts - count));

		StringBuilder outputObj = new StringBuilder();
		outputObj.append(sumJaccard + ",");
		outputObj.append(sumCosine);
//		outputObj.append(sumCosine + ",");
//		outputObj.append(keyArray[0] + ",");
//		outputObj.append(keyArray[1]);
		Text t = new Text();
		t.set(outputObj.toString());

		context.write(ikey, t);
	}
}
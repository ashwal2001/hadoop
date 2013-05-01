package com.xyz.reccommendation.reducer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.io.BSONWritable;

public class IntSumReducer extends
		Reducer<Text, IntWritable, Text, BSONWritable> {

	private static final Log log = LogFactory.getLog(IntSumReducer.class);

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		log.debug("Key : " + key.toString());
		int sum = 0;
		for (final IntWritable val : values) {
			sum += val.get();
		}

		String[] keyArray = key.toString().split("\\|");

		log.debug("Count : " + sum + " p1 : " + keyArray[0] + " p2 : "
				+ keyArray[1]);

		BasicBSONObject output = new BasicBSONObject();
		output.put("count", sum);
		output.put("p1", keyArray[0]);
		output.put("p2", keyArray[1]);

		context.write(key, new BSONWritable(output));
	}
}
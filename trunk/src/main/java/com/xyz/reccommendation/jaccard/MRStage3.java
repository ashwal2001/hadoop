package com.xyz.reccommendation.jaccard;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mongodb.hadoop.io.BSONWritable;
import com.xyz.reccommendation.mapper.PairCollectMap;
import com.xyz.reccommendation.reducer.NormalizationReduce;

public class MRStage3 {

	private static final Log log = LogFactory.getLog(MRStage3.class);

	public static void main(String[] args) throws Exception {

		// Job configuration
		final Configuration conf = new Configuration();
		log.debug("Conf: " + conf);

		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf, "Map-Reduce 3 with HDFS intermediate 3");

		job.setJarByClass(MRStage3.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);

		job.setMapperClass(PairCollectMap.class);
		job.setReducerClass(NormalizationReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// HDFS input and output directory
		FileInputFormat.setInputPaths(job, new Path("intermediate1"));
		FileOutputFormat.setOutputPath(job, new Path("intermediate2"));

		// Run map-reduce job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

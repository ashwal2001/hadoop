package com.xyz.reccommendation.driver;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xyz.reccommendation.comparator.RecoEngineKeyComparator;
import com.xyz.reccommendation.grouping.RecoEngineGroupingComparator;
import com.xyz.reccommendation.key.RecoEngineCompositeKey;
import com.xyz.reccommendation.mapper.RecoEngineMapper;
import com.xyz.reccommendation.partitioner.RecoEngineCustomPartitioner;
import com.xyz.reccommendation.reducer.RecoEngineReducer;

public class RecoEngine {

	private static final Log log = LogFactory.getLog(RecoEngine.class);

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();

		String envt = null;

//		if (args.length < 1) {
//			System.err.printf("Usage: %s <config file>\n", getClass()
//					.getSimpleName());
//			System.exit(1);
//		}

		if (args.length > 0) {
			envt = args[0];
		} else {
			envt = "dev";
		}

		Properties prop = new Properties();

		try {
			// load a properties file from class path, inside static method
			prop.load(RecoEngine.class.getClassLoader().getResourceAsStream(
					"config-" + envt + ".properties"));

		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(1);
		}

		MongoConfigUtil.setInputURI(
				conf,
				"mongodb://" + prop.getProperty("mongodb.ip") + "/"
						+ prop.getProperty("mongodb.dbname") + "."
						+ prop.getProperty("mongodb.collectionname.input"));

		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		log.debug("Conf: " + conf);

		final Job job = new Job(conf,
				"MR Job for grouping records as per session id and time");

		job.setJarByClass(RecoEngine.class);

		job.setMapperClass(RecoEngineMapper.class);

		// job.setCombinerClass(RecoEngineReducer.class);
		job.setReducerClass(RecoEngineReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(RecoEngineCompositeKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(RecoEngineCustomPartitioner.class);
		job.setGroupingComparatorClass(RecoEngineGroupingComparator.class);
		job.setSortComparatorClass(RecoEngineKeyComparator.class);

		FileOutputFormat.setOutputPath(job, new Path("inputPview"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
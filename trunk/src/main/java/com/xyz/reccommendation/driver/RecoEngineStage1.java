package com.xyz.reccommendation.driver;

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

import com.xyz.reccommendation.comparator.RecoEngineKeyComparator;
import com.xyz.reccommendation.grouping.RecoEngineGroupingComparator;
import com.xyz.reccommendation.key.RecoEngineCompositeKey;
import com.xyz.reccommendation.mapper.RecoEngineMapperCSV;
import com.xyz.reccommendation.partitioner.RecoEngineCustomPartitioner;
import com.xyz.reccommendation.reducer.RecoEngineReducer;

public class RecoEngineStage1 {

	private static final Log log = LogFactory.getLog(RecoEngineStage1.class);

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();

		// String envt = null;

		if (args.length < 2) {
			System.err.printf(
					"Usage: %s <input-hdfs-path> <output-hdfs-path>\n",
					RecoEngineStage1.class.getSimpleName());
			System.exit(1);
		}

		// if (args.length > 0) {
		// envt = args[0];
		// } else {
		// envt = "dev";
		// }
		//
		// Properties prop = new Properties();
		//
		// try {
		// // load a properties file from class path, inside static method
		// prop.load(RecoEngineStage1.class.getClassLoader().getResourceAsStream(
		// "config-" + envt + ".properties"));
		//
		// } catch (IOException ex) {
		// ex.printStackTrace();
		// System.exit(1);
		// }

		// MongoConfigUtil.setInputURI(
		// conf,
		// "mongodb://" + prop.getProperty("mongodb.ip") + "/"
		// + prop.getProperty("mongodb.dbname") + "."
		// + prop.getProperty("mongodb.collectionname.input"));
		//
		// log.debug("MongoDB URL : mongodb://" + prop.getProperty("mongodb.ip")
		// + "/" + prop.getProperty("mongodb.dbname") + "."
		// + prop.getProperty("mongodb.collectionname.input"));
		//
		// MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		log.debug("Conf: " + conf);

		final Job job = new Job(conf,
				"MR Job for grouping records as per session id and time");

		job.setJarByClass(RecoEngineStage1.class);

		job.setMapperClass(RecoEngineMapperCSV.class);

		// job.setCombinerClass(RecoEngineReducer.class);
		job.setReducerClass(RecoEngineReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setInputFormatClass(MongoInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(RecoEngineCompositeKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(RecoEngineCustomPartitioner.class);
		job.setGroupingComparatorClass(RecoEngineGroupingComparator.class);
		job.setSortComparatorClass(RecoEngineKeyComparator.class);

		// FileInputFormat.setInputPaths(job, new Path("input"));
		// FileOutputFormat.setOutputPath(job, new Path("inputPview"));

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
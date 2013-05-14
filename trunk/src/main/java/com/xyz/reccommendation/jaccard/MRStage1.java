package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.Properties;

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

import com.xyz.reccommendation.mapper.XYIdentityMapCSV;
import com.xyz.reccommendation.reducer.CountYReduce;

public class MRStage1 {

	private static final Log log = LogFactory.getLog(MRStage1.class);

	public static void main(String[] args) throws Exception {
		// Job configuration
		final Configuration conf = new Configuration();

		String envt = null;

		if (args.length > 0) {
			envt = args[0];
		} else {
			envt = "dev";
		}
		log.debug("Envt: " + envt);
		Properties prop = new Properties();

		try {
			// load a properties file from class path, inside static method
			prop.load(MRStage1.class.getClassLoader().getResourceAsStream(
					"config-" + envt + ".properties"));

		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(1);
		}

		// MongoConfigUtil.setInputURI(
		// conf,
		// "mongodb://" + prop.getProperty("mongodb.ip") + "/"
		// + prop.getProperty("mongodb.dbname") + "."
		// + prop.getProperty("mongodb.collectionname.input"));
		//
		// log.debug("MongoDB URL : mongodb://" + prop.getProperty("mongodb.ip")
		// + "/"
		// + prop.getProperty("mongodb.dbname") + "."
		// + prop.getProperty("mongodb.collectionname.input"));
		//
		// MongoConfigUtil.setCreateInputSplits(conf, false);

		log.debug("Conf: " + conf);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf, "Map-Reduce Stage 1");

		job.setJarByClass(MRStage1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(XYIdentityMapCSV.class);
		job.setReducerClass(CountYReduce.class);

		// job.setInputFormatClass(MongoInputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// HDFS input and output directory
		FileInputFormat.setInputPaths(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("intermediate0"));

		// Run map-reduce job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

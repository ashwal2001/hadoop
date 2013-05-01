package com.xyz.reccommendation.jaccard;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bson.BasicBSONObject;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xyz.reccommendation.jaccard.key.CompositeKeyComparator;
import com.xyz.reccommendation.jaccard.key.CompositeKeyGroupingComparator;
import com.xyz.reccommendation.jaccard.key.CompositeKeyPartitioner;
import com.xyz.reccommendation.jaccard.key.TextDoublePair;

public class JaccardMRStage4 {
	/**
	 * Map to collect pairs
	 */
	public static class JaccardMap extends
			Mapper<LongWritable, Text, TextDoublePair, Text> {
		/**
		 * @param ikey
		 *            Dummy parameter required by Hadoop for the map operation,
		 *            but it does nothing.
		 * @param ival
		 *            Text used to read in necessary data, in this case each
		 *            line of the text will be a list of CPairs
		 * @param output
		 *            OutputCollector that collects the output of the map
		 *            operation, in this case a (Text, IntWritable) pair
		 *            representing (
		 * @param reporter
		 *            Used by Hadoop, but we do not use it
		 */
		public void map(LongWritable ikey, Text ival, Context context)
				throws IOException, InterruptedException {
			String[] token = ival.toString().split("\\,+");
			TextDoublePair okey = new TextDoublePair();
			okey.set(token[3], Double.parseDouble(token[1]));
			context.write(okey, ival);
		}
	}

	/**
	 * Reduce class that does the final normalization. Takes in data of the form
	 * (xi xj (sum of counts)) Outputs the similarity between xi and xj as (the
	 * number of y values xi and xj are associated with)/(the sum of the counts)
	 */
	public static class JaccardReduce extends
			Reducer<TextDoublePair, Text, Text, BSONWritable> {
		/**
		 * @param ikey
		 *            The xi xj pair for which we are calculating the similarity
		 * @param vlist
		 *            The list of ((xi,xj),sum of counts) pairs
		 * @param output
		 *            OutputCollector that collects a (Text, FloatWritable)
		 *            pair. where the Text is the (xi,xj) pair and the
		 *            FloatWritable is their similarity
		 * @param reporter
		 *            Used by Hadoop, but we do not use it
		 */
		public void reduce(Text ikey, Iterable<Text> vlist, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			while (vlist.iterator().hasNext() && count < 20) {
				Text val = vlist.iterator().next();
				String[] token = val.toString().split("\\,+");

				BasicBSONObject outputObj = new BasicBSONObject();
				outputObj.put("countJaccard", token[1]);
				outputObj.put("p1", token[3]);
				outputObj.put("p2", token[4]);
				Text id = new Text(token[0].trim());

				context.write(id, new BSONWritable(outputObj));// 7294, 50000,
																// jul 2013
				count++;
			}

		}

	}

	private static final Log log = LogFactory.getLog(JaccardMRStage4.class);

	public static void main(String[] args) throws Exception {

		// Job configuration
		final Configuration conf = new Configuration();

		String envt = null;

		if (args.length > 0) {
			envt = args[0];
		} else {
			envt = "dev";
		}

		Properties prop = new Properties();

		try {
			// load a properties file from class path, inside static method
			prop.load(JaccardMRStage4.class.getClassLoader()
					.getResourceAsStream("config-" + envt + ".properties"));

		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(1);
		}

		log.debug("Conf: " + conf);

		MongoConfigUtil.setOutputURI(
				conf,
				"mongodb://" + prop.getProperty("mongodb.ip") + "/"
						+ prop.getProperty("mongodb.dbname")
						+ ".out_stat_jaccard");

		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf, "Map-Reduce for Jaccard 4");

		job.setJarByClass(JaccardMRStage4.class);

		job.setMapOutputKeyClass(TextDoublePair.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);

		job.setMapperClass(JaccardMap.class);
		job.setReducerClass(JaccardReduce.class);

		job.setPartitionerClass(CompositeKeyPartitioner.class);
		job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		// HDFS input and output directory
		FileInputFormat.setInputPaths(job, new Path("intermediate2"));

		// Run map-reduce job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

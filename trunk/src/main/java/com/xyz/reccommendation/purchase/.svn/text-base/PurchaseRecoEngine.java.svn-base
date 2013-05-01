package com.xyz.reccommendation.purchase;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xyz.reccommendation.mapper.SalesMapper;
import com.xyz.reccommendation.reducer.IntSumReducer;

public class PurchaseRecoEngine {

	private static final Log log = LogFactory.getLog(PurchaseRecoEngine.class);

	public static void main(String[] args) throws Exception {

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
			prop.load(PurchaseRecoEngine.class.getClassLoader()
					.getResourceAsStream("config-" + envt + ".properties"));

		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(1);
		}

		// set the properties into Config object so it is available for Mappers
		// and Reducers
		@SuppressWarnings("rawtypes")
		Enumeration keys = prop.propertyNames();
		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			String value = prop.getProperty(key);
			conf.set(key, value);
		}
		conf.set("sku2sku.count.weight", "1");

		log.debug("Conf: " + conf);

		MongoConfigUtil.setOutputURI(
				conf,
				"mongodb://" + prop.getProperty("mongodb.ip") + "/"
						+ prop.getProperty("mongodb.dbname")
						+ ".out_stat_purchase");
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf, "SKU To SKU count in purchase data");

		job.setJarByClass(PurchaseRecoEngine.class);

		job.setMapperClass(SalesMapper.class);

		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("inputPurchase"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

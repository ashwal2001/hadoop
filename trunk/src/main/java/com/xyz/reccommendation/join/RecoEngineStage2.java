/*
 * ===========================================================================
 * RecoEngine.java
 *
 * Created on 01-May-2013
 *
 * This code contains copyright information which is the proprietary property
 * of Jade eServices. No part of this code may be reproduced, stored or transmitted
 * in any form without the prior written permission of Jade eServices.
 *
 * Copyright (C) Jade eServices. 2013
 * All rights reserved.
 *
 * Modification history:
 * $Log: RecoEngine.java,v $
 * ===========================================================================
 */
package com.xyz.reccommendation.join;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.xyz.reccommendation.mapper.SalesMapper;
import com.xyz.reccommendation.mapper.TokenizerMapper;
import com.xyz.reccommendation.reducer.IntSumReducerCSV;

/**
 * @author ashok
 * 
 * @version $Id: RecoEngine.java,v 1.1 01-May-2013 11:29:48 AM ashok Exp $
 */
public class RecoEngineStage2 {

	private static final Log log = LogFactory.getLog(RecoEngineStage2.class);

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();

		if (args.length < 2) {
			System.err.printf(
					"Usage: %s <input-hdfs-path1> <input-hdfs-path2> <output-hdfs-path>\n",
					RecoEngineStage2.class.getSimpleName());
			System.exit(1);
		}

		// String envt = null;
		//
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
		// prop.load(RecoEngineStage2.class.getClassLoader()
		// .getResourceAsStream("config-" + envt + ".properties"));
		//
		// } catch (IOException ex) {
		// ex.printStackTrace();
		// System.exit(1);
		// }

		// set the properties into Config object so it is available for Mappers
		// and Reducers
		// @SuppressWarnings("rawtypes")
		// Enumeration keys = prop.propertyNames();
		// while (keys.hasMoreElements()) {
		// String key = (String) keys.nextElement();
		// String value = prop.getProperty(key);
		// conf.set(key, value);
		// }
		conf.set("sku2sku.count.weight", "10");
		log.debug("Conf: " + conf);

		// MongoConfigUtil.setOutputURI(
		// conf,
		// "mongodb://" + prop.getProperty("mongodb.ip") + "/"
		// + prop.getProperty("mongodb.dbname")
		// + ".out_stat_join_s3");
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf,
				"SKU To SKU count in purchase and pview data");

		job.setJarByClass(RecoEngineStage2.class);

		job.setReducerClass(IntSumReducerCSV.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(BSONWritable.class);

		// job.setOutputFormatClass(MongoOutputFormat.class);

		// MultipleInputs.addInputPath(job, new Path("inputPview"),
		// KeyValueTextInputFormat.class, TokenizerMapper.class);
		// MultipleInputs.addInputPath(job, new Path("inputPurchase"),
		// TextInputFormat.class, SalesMapper.class);

		job.setOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),
				KeyValueTextInputFormat.class, TokenizerMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, SalesMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

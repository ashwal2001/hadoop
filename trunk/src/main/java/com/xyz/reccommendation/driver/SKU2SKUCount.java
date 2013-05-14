// SKU2SKUCount.java
/*
 * Copyright 2013 Jade eServices.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xyz.reccommendation.driver;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.xyz.reccommendation.mapper.TokenizerMapper;
import com.xyz.reccommendation.reducer.IntSumReducer;

/**
 * 
 */
public class SKU2SKUCount {

	private static final Log log = LogFactory.getLog(SKU2SKUCount.class);

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
			prop.load(SKU2SKUCount.class.getClassLoader().getResourceAsStream(
					"config-" + envt + ".properties"));

		} catch (IOException ex) {
			ex.printStackTrace();
			System.exit(1);
		}

		MongoConfigUtil.setOutputURI(
				conf,
				"mongodb://" + prop.getProperty("mongodb.ip") + "/"
						+ prop.getProperty("mongodb.dbname")
						+ ".out_stat_custom");
		
		log.debug("MongoDB URL : mongodb://" + prop.getProperty("mongodb.ip") + "/"
				+ prop.getProperty("mongodb.dbname") + "."
				+ ".out_stat_custom");

		log.debug("Conf: " + conf);

		MongoConfigUtil.setCreateInputSplits(conf, false);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();

		final Job job = new Job(conf,
				"Count the sku to sku mapping from pview data on hdfs in \"inputPview\" path.");

		job.setJarByClass(SKU2SKUCount.class);

		job.setMapperClass(TokenizerMapper.class);

		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BSONWritable.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(MongoOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("inputPview"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

package com.xyz.reccommendation.driver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.xyz.reccommendation.comparator.RecoEngineKeyComparator;
import com.xyz.reccommendation.grouping.RecoEngineGroupingComparator;
import com.xyz.reccommendation.join.RecoEngineStage2;
import com.xyz.reccommendation.key.RecoEngineCompositeKey;
import com.xyz.reccommendation.mapper.RecoEngineMapperCSV;
import com.xyz.reccommendation.mapper.SalesMapper;
import com.xyz.reccommendation.mapper.TokenizerMapper;
import com.xyz.reccommendation.partitioner.RecoEngineCustomPartitioner;
import com.xyz.reccommendation.reducer.IntSumReducerCSV;
import com.xyz.reccommendation.reducer.RecoEngineReducer;

public class RecoEngine {

	private static final Log log = LogFactory.getLog(RecoEngine.class);

	public static void main(String[] args) {

		try {

			final Configuration conf1 = new Configuration();

			if (args.length < 2) {
				System.err.printf(
						"Usage: %s <input-hdfs-path> <output-hdfs-path>\n",
						RecoEngineStage1.class.getSimpleName());
				System.exit(1);
			}

			args = new GenericOptionsParser(conf1, args).getRemainingArgs();
			log.debug("Conf: " + conf1);

			final Job job1 = new Job(conf1,
					"MR Job for grouping records as per session id and time");

			job1.setJarByClass(RecoEngineStage1.class);

			job1.setMapperClass(RecoEngineMapperCSV.class);

			job1.setReducerClass(RecoEngineReducer.class);

			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);

			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			job1.setMapOutputKeyClass(RecoEngineCompositeKey.class);
			job1.setMapOutputValueClass(Text.class);

			job1.setPartitionerClass(RecoEngineCustomPartitioner.class);
			job1.setGroupingComparatorClass(RecoEngineGroupingComparator.class);
			job1.setSortComparatorClass(RecoEngineKeyComparator.class);

			FileInputFormat.addInputPath(job1, new Path(args[0]));
			String out = args[1] + System.nanoTime();
			FileOutputFormat.setOutputPath(job1, new Path(out));
			
			final Configuration conf2 = new Configuration();

			if (args.length < 2) {
				System.err.printf(
						"Usage: %s <input-hdfs-path1> <input-hdfs-path2> <output-hdfs-path>\n",
						RecoEngineStage2.class.getSimpleName());
				System.exit(1);
			}

			conf2.set("sku2sku.count.weight", "10");
			
			log.debug("Conf: " + conf2);
			
			args = new GenericOptionsParser(conf2, args).getRemainingArgs();

			final Job job2 = new Job(conf2,
					"SKU To SKU count in purchase and pview data");

			job2.setJarByClass(RecoEngineStage2.class);

			job2.setReducerClass(IntSumReducerCSV.class);

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);

			job2.setOutputKeyClass(Text.class);

			job2.setOutputValueClass(Text.class);

			job2.setOutputFormatClass(TextOutputFormat.class);

			MultipleInputs.addInputPath(job2, new Path(out),
					KeyValueTextInputFormat.class, TokenizerMapper.class);
			MultipleInputs.addInputPath(job2, new Path(args[1]),
					TextInputFormat.class, SalesMapper.class);
			
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
			ControlledJob controlledJob1 = new ControlledJob(
					job1.getConfiguration());
			ControlledJob controlledJob2 = new ControlledJob(
					job2.getConfiguration());
			controlledJob2.addDependingJob(controlledJob1);
			JobControl jobControl = new JobControl("control");
			jobControl.addJob(controlledJob1);
			jobControl.addJob(controlledJob2);
			Thread thread = new Thread(jobControl);
			thread.start();
			while (!jobControl.allFinished()) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			jobControl.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

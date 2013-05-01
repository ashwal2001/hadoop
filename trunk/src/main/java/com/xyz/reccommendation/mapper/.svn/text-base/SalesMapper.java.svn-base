package com.xyz.reccommendation.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.xyz.reccommendation.util.StringUtils;

public class SalesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final Log log = LogFactory.getLog(SalesMapper.class);

	private final Text word = new Text();

	public void map(LongWritable offset, Text text, Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String weight = conf.get("sku2sku.count.weight");

		String[] array = text.toString().split("\\,");
		for (int i = 1; i < array.length - 1; i++) {
			if (StringUtils.isValid(array[i])
					&& StringUtils.isValid(array[i + 1])) {
				String sku1 = StringUtils.getSKU(array[i]);
				String sku2 = StringUtils.getSKU(array[i + 1]);
				if (!sku1.equals(sku2)) {
					word.set(sku1 + "|" + sku2);
					context.write(word,
							new IntWritable(Integer.parseInt(weight)));
				} else {
					log.debug("## Same ##" + array[i] + " " + array[i + 1]);
				}
			} else {
				log.debug("## Invalid ##" + array[i] + " " + array[i + 1]);
			}
		}
	}
}

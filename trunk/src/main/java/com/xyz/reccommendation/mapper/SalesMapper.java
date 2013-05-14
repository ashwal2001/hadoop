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

		for (int m = 1; m < array.length - 1; m++) {
			for (int i = m + 1; i < array.length; i++) {
				if (StringUtils.isValid(array[m])
						&& StringUtils.isValid(array[i])) {
					String sku1 = StringUtils.getSKU(array[m]);
					String sku2 = StringUtils.getSKU(array[i]);
					if (!sku1.equals(sku2)) {
						word.set(sku1 + "|" + sku2);
						context.write(word,
								new IntWritable(Integer.parseInt(weight)));
					} else {
						log.debug("## Same ##" + array[m] + " " + array[i]);
					}
				} else {
					log.debug("## Invalid ##" + array[m] + " " + array[i]);
				}
			}
		}
	}
}

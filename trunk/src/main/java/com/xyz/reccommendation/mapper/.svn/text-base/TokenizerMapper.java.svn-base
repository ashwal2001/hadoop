package com.xyz.reccommendation.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.xyz.reccommendation.util.StringUtils;

public class TokenizerMapper extends Mapper<Text, Text, Text, IntWritable> {

	private static final Log log = LogFactory.getLog(TokenizerMapper.class);

	private final static IntWritable one = new IntWritable(1);
	private final Text word = new Text();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {

		String[] array = value.toString().split("\\,");
		for (int i = 0; i < array.length - 1; i++) {
			if (StringUtils.isValid(array[i])
					&& StringUtils.isValid(array[i + 1])) {
				if (!array[i].equals(array[i + 1])) {
					String sku1 = array[i].replaceAll("\"", "");
					String sku2 = array[i + 1].replaceAll("\"", "");
					word.set(sku1 + "|" + sku2);
					log.debug(sku1 + "|" + sku2);
					context.write(word, one);
				} else {
					log.debug("## Same ##" + array[i] + " " + array[i + 1]);
				}
			} else {
				log.debug("## Invalid ##" + array[i] + " " + array[i + 1]);
			}
		}
	}
}

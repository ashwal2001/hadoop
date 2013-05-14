package com.xyz.reccommendation.mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.xyz.reccommendation.key.RecoEngineCompositeKey;
import com.xyz.reccommendation.util.StringUtils;

public class RecoEngineMapperCSV extends
		Mapper<LongWritable, Text, RecoEngineCompositeKey, Text> {

	private static final Log log = LogFactory.getLog(RecoEngineMapperCSV.class);

	private RecoEngineCompositeKey compositeKey = new RecoEngineCompositeKey();
	private Text valueSku = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] token = value.toString().split("\\,");

		// sku,sessionId,time

		log.debug("key: " + key + " value: " + value);
		if (token.length == 3 && StringUtils.isValid(token[0])
				&& StringUtils.isValid(token[1])
				&& StringUtils.isValid(token[2])) {
			String date = null;
			try {
				date = getDate(token[2]);
			} catch (ParseException e) {
				System.err.println(token[0] + " : " + token[1] + " : "
						+ token[2]);
			}
			if (date != null) {
				compositeKey.setDatetime(date);
				compositeKey.setSessionId(token[1]);

				valueSku.set(token[0]);
				log.debug(token[0] + " : " + token[1] + " : " + date);
				context.write(compositeKey, valueSku);
			}
		}
	}

	public static String getDate(String dateStr) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss'Z'");
		format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "IST")));
		Date date = null;
		date = format.parse(dateStr);
		return date.toString();
	}
}

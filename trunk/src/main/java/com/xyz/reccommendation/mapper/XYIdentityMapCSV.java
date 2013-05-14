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

import com.xyz.reccommendation.util.StringUtils;

/**
 * Map class that maps a text input of x y to a text output of x y
 */
public class XYIdentityMapCSV extends Mapper<LongWritable, Text, Text, Text> {

	private static final Log log = LogFactory.getLog(XYIdentityMapCSV.class);

	/**
	 * @param ikey
	 *            Dummy parameter required by Hadoop for the map operation, but
	 *            it does nothing.
	 * @param ival
	 *            Text used to read in necessary data, in this case each line of
	 *            the text will be in the format of ( x y )
	 * @param output
	 *            OutputCollector that collects the output of the map operation,
	 *            in this case a (Text, Text) pair representing the (x,y)
	 * @param reporter
	 *            Used by Hadoop, but we do not use it
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] token = value.toString().split("\\,");

		// sku,sessionId,time

		log.debug("key: " + key + " value: " + value);
		if (token.length == 3 && StringUtils.isValid(token[0])
				&& StringUtils.isValid(token[1])
				&& StringUtils.isValid(token[2])) {
			log.debug(token[0] + " : " + token[1] + " : " + getDate(token[2]));
			Text okey = new Text(token[0]);
			Text oval = new Text(token[1]);
			context.write(okey, oval);
		}
	}

	public static String getDate(String dateStr) {
		SimpleDateFormat format = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss'Z'");
		format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "IST")));
		Date date = null;
		try {
			date = format.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date.toString();
	}
}
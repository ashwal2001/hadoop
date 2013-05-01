package com.xyz.reccommendation.mapper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.xyz.reccommendation.key.RecoEngineCompositeKey;

public class RecoEngineMapper extends
		Mapper<Object, BSONObject, RecoEngineCompositeKey, Text> {

	private static final Log log = LogFactory.getLog(RecoEngineMapper.class);

	private RecoEngineCompositeKey compositeKey = new RecoEngineCompositeKey();
	private Text valueSku = new Text();

	@Override
	public void map(Object key, BSONObject value, Context context)
			throws IOException, InterruptedException {

		log.debug("key: " + key + " value: " + value);
		if (value.get("time") != null && value.get("sessionId") != null
				&& value.get("sku") != null) {
			compositeKey.setDatetime(value.get("time").toString());
			compositeKey.setSessionId(value.get("sessionId").toString());

			valueSku.set(value.get("sku").toString());
			log.debug(value.get("sessionId").toString() + " : "
					+ value.get("sku"));
			context.write(compositeKey, valueSku);
		}

	}

}

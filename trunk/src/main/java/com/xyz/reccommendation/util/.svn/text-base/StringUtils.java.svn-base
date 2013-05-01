package com.xyz.reccommendation.util;

public class StringUtils {
	
	public static boolean isValid(final String string) {
		return string != null && !string.isEmpty() && !string.trim().isEmpty();
	}

	public static String getSKU(final String skuId) {
		String sku = null;
		String[] articleSku = skuId.split("\\-");
		sku = articleSku[0].replaceAll("\"", "");
		return sku;
	}
}

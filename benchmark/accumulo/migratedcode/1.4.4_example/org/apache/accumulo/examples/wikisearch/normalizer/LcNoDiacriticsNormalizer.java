package org.apache.accumulo.examples.wikisearch.normalizer;


import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.text.Normalizer.Form.NFC;
import static java.text.Normalizer.Form.NFD;
import static java.text.Normalizer.normalize;


public class LcNoDiacriticsNormalizer implements Normalizer {
	private static final Pattern diacriticals = Pattern.compile("\\p{InCombiningDiacriticalMarks}");

	public String normalizeFieldValue(String fieldName, Object fieldValue) {
		String decomposed = normalize(fieldValue.toString(), NFD);
		String noDiacriticals = removeDiacriticalMarks(decomposed);
		String recomposed = normalize(noDiacriticals, NFC);
		return recomposed.toLowerCase(Locale.ENGLISH);
	}

	private String removeDiacriticalMarks(String str) {
		Matcher matcher = LcNoDiacriticsNormalizer.diacriticals.matcher(str);
		return matcher.replaceAll("");
	}
}


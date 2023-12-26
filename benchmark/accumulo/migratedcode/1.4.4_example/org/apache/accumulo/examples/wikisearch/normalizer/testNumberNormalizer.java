package org.apache.accumulo.examples.wikisearch.normalizer;


import org.junit.Assert;


public class testNumberNormalizer {
	@org.junit.Test
	public void test1() throws Exception {
		NumberNormalizer nn = new NumberNormalizer();
		String n1 = nn.normalizeFieldValue(null, "1");
		String n2 = nn.normalizeFieldValue(null, "1.00000000");
		Assert.assertTrue(((n1.compareTo(n2)) < 0));
	}

	@org.junit.Test
	public void test2() {
		NumberNormalizer nn = new NumberNormalizer();
		String n1 = nn.normalizeFieldValue(null, "-1.0");
		String n2 = nn.normalizeFieldValue(null, "1.0");
		Assert.assertTrue(((n1.compareTo(n2)) < 0));
	}

	@org.junit.Test
	public void test3() {
		NumberNormalizer nn = new NumberNormalizer();
		String n1 = nn.normalizeFieldValue(null, "-0.0001");
		String n2 = nn.normalizeFieldValue(null, "0");
		String n3 = nn.normalizeFieldValue(null, "0.00001");
		Assert.assertTrue((((n1.compareTo(n2)) < 0) && ((n2.compareTo(n3)) < 0)));
	}

	@org.junit.Test
	public void test4() {
		NumberNormalizer nn = new NumberNormalizer();
		String nn1 = nn.normalizeFieldValue(null, Integer.toString(Integer.MAX_VALUE));
		String nn2 = nn.normalizeFieldValue(null, Integer.toString(((Integer.MAX_VALUE) - 1)));
		Assert.assertTrue(((nn2.compareTo(nn1)) < 0));
	}

	@org.junit.Test
	public void test5() {
		NumberNormalizer nn = new NumberNormalizer();
		String nn1 = nn.normalizeFieldValue(null, "-0.001");
		String nn2 = nn.normalizeFieldValue(null, "-0.0009");
		String nn3 = nn.normalizeFieldValue(null, "-0.00090");
		Assert.assertTrue((((nn3.compareTo(nn2)) == 0) && ((nn2.compareTo(nn1)) > 0)));
	}

	@org.junit.Test
	public void test6() {
		NumberNormalizer nn = new NumberNormalizer();
		String nn1 = nn.normalizeFieldValue(null, "00.0");
		String nn2 = nn.normalizeFieldValue(null, "0");
		String nn3 = nn.normalizeFieldValue(null, "0.0");
		Assert.assertTrue((((nn3.compareTo(nn2)) == 0) && ((nn2.compareTo(nn1)) == 0)));
	}
}


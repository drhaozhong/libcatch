package org.apache.accumulo.examples.simple.filedata;


import junit.framework.TestCase;
import org.apache.hadoop.io.Text;


public class KeyUtilTest extends TestCase {
	public static void checkSeps(String... s) {
		Text t = KeyUtil.buildNullSepText(s);
		String[] rets = KeyUtil.splitNullSepText(t);
		int length = 0;
		for (String str : s)
			length += str.length();

		TestCase.assertEquals(t.getLength(), ((length + (s.length)) - 1));
		TestCase.assertEquals(rets.length, s.length);
		for (int i = 0; i < (s.length); i++)
			TestCase.assertEquals(s[i], rets[i]);

	}

	public void testNullSep() {
		KeyUtilTest.checkSeps("abc", "d", "", "efgh");
		KeyUtilTest.checkSeps("ab", "");
		KeyUtilTest.checkSeps("abcde");
		KeyUtilTest.checkSeps("");
		KeyUtilTest.checkSeps("", "");
	}
}


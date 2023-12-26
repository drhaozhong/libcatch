package org.apache.accumulo.examples.simple.filedata;


import java.util.ArrayList;
import org.apache.hadoop.io.Text;


public class KeyUtil {
	public static final byte[] nullbyte = new byte[]{ 0 };

	public static Text buildNullSepText(String... s) {
		Text t = new Text(s[0]);
		for (int i = 1; i < (s.length); i++) {
			t.append(KeyUtil.nullbyte, 0, 1);
			t.append(s[i].getBytes(), 0, s[i].length());
		}
		return t;
	}

	public static String[] splitNullSepText(Text t) {
		ArrayList<String> s = new ArrayList<>();
		byte[] b = t.getBytes();
		int lastindex = 0;
		for (int i = 0; i < (t.getLength()); i++) {
			if ((b[i]) == ((byte) (0))) {
				s.add(new String(b, lastindex, (i - lastindex)));
				lastindex = i + 1;
			}
		}
		s.add(new String(b, lastindex, ((t.getLength()) - lastindex)));
		return s.toArray(new String[s.size()]);
	}
}


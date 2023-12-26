package org.apache.accumulo.examples.wikisearch.util;


import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.hadoop.io.Text;


public class TextUtil {
	public static void textAppend(Text text, String string) {
		TextUtil.appendNullByte(text);
		TextUtil.textAppendNoNull(text, string);
	}

	public static void textAppend(Text text, String string, boolean replaceBadChar) {
		TextUtil.appendNullByte(text);
		TextUtil.textAppendNoNull(text, string, replaceBadChar);
	}

	public static void textAppend(Text t, long s) {
		t.append(TextUtil.nullByte, 0, 1);
		t.append(SummingCombiner.FIXED_LEN_ENCODER.encode(s), 0, 8);
	}

	private static final byte[] nullByte = new byte[]{ 0 };

	public static void appendNullByte(Text text) {
		text.append(TextUtil.nullByte, 0, TextUtil.nullByte.length);
	}

	public static void textAppendNoNull(Text t, String s) {
		TextUtil.textAppendNoNull(t, s, false);
	}

	public static void textAppendNoNull(Text t, String s, boolean replaceBadChar) {
		try {
			ByteBuffer buffer = Text.encode(s, replaceBadChar);
			t.append(buffer.array(), 0, buffer.limit());
		} catch (CharacterCodingException cce) {
			throw new IllegalArgumentException(cce);
		}
	}

	public static byte[] toUtf8(String string) {
		ByteBuffer buffer;
		try {
			buffer = Text.encode(string, false);
		} catch (CharacterCodingException cce) {
			throw new IllegalArgumentException(cce);
		}
		byte[] bytes = new byte[buffer.limit()];
		System.arraycopy(buffer.array(), 0, bytes, 0, bytes.length);
		return bytes;
	}
}


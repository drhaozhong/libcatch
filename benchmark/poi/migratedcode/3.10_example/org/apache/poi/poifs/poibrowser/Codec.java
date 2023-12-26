package org.apache.poi.poifs.poibrowser;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import org.apache.poi.hpsf.ClassID;


public class Codec {
	protected static final byte[] hexval = new byte[]{ ((byte) ('0')), ((byte) ('1')), ((byte) ('2')), ((byte) ('3')), ((byte) ('4')), ((byte) ('5')), ((byte) ('6')), ((byte) ('7')), ((byte) ('8')), ((byte) ('9')), ((byte) ('A')), ((byte) ('B')), ((byte) ('C')), ((byte) ('D')), ((byte) ('E')), ((byte) ('F')) };

	public static String hexEncode(final String s) {
		return Codec.hexEncode(s.getBytes());
	}

	public static String hexEncode(final byte[] s) {
		return Codec.hexEncode(s, 0, s.length);
	}

	public static String hexEncode(final byte[] s, final int offset, final int length) {
		StringBuffer b = new StringBuffer((length * 2));
		for (int i = offset; i < (offset + length); i++) {
			int c = s[i];
			b.append(((char) (Codec.hexval[((c & 240) >> 4)])));
			b.append(((char) (Codec.hexval[((c & 15) >> 0)])));
		}
		return b.toString();
	}

	public static String hexEncode(final byte b) {
		StringBuffer sb = new StringBuffer(2);
		sb.append(((char) (Codec.hexval[((b & 240) >> 4)])));
		sb.append(((char) (Codec.hexval[((b & 15) >> 0)])));
		return sb.toString();
	}

	public static String hexEncode(final short s) {
		StringBuffer sb = new StringBuffer(4);
		sb.append(((char) (Codec.hexval[((s & 61440) >> 12)])));
		sb.append(((char) (Codec.hexval[((s & 3840) >> 8)])));
		sb.append(((char) (Codec.hexval[((s & 240) >> 4)])));
		sb.append(((char) (Codec.hexval[((s & 15) >> 0)])));
		return sb.toString();
	}

	public static String hexEncode(final int i) {
		StringBuffer sb = new StringBuffer(8);
		sb.append(((char) (Codec.hexval[((i & -268435456) >> 28)])));
		sb.append(((char) (Codec.hexval[((i & 251658240) >> 24)])));
		sb.append(((char) (Codec.hexval[((i & 15728640) >> 20)])));
		sb.append(((char) (Codec.hexval[((i & 983040) >> 16)])));
		sb.append(((char) (Codec.hexval[((i & 61440) >> 12)])));
		sb.append(((char) (Codec.hexval[((i & 3840) >> 8)])));
		sb.append(((char) (Codec.hexval[((i & 240) >> 4)])));
		sb.append(((char) (Codec.hexval[((i & 15) >> 0)])));
		return sb.toString();
	}

	public static String hexEncode(final long l) {
		StringBuffer sb = new StringBuffer(16);
		sb.append(Codec.hexEncode((((int) (l & -4294967296L)) >> 32)));
		sb.append(Codec.hexEncode((((int) (l & 4294967295L)) >> 0)));
		return sb.toString();
	}

	public static String hexEncode(final ClassID classID) {
		return Codec.hexEncode(classID.getBytes());
	}

	public static byte[] hexDecode(final String s) {
		final int length = s.length();
		if ((length % 2) == 1)
			throw new IllegalArgumentException(("String has odd length " + length));

		byte[] b = new byte[length / 2];
		char[] c = new char[length];
		s.toUpperCase().getChars(0, length, c, 0);
		for (int i = 0; i < length; i += 2)
			b[(i / 2)] = ((byte) ((((Codec.decodeNibble(c[i])) << 4) & 240) | ((Codec.decodeNibble(c[(i + 1)])) & 15)));

		return b;
	}

	protected static byte decodeNibble(final char c) {
		for (byte i = 0; i < (Codec.hexval.length); i++)
			if (((byte) (c)) == (Codec.hexval[i]))
				return i;


		throw new IllegalArgumentException(((("\"" + c) + "\"") + " does not represent a nibble."));
	}

	public static void main(final String[] args) throws IOException {
		final BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		String s;
		do {
			s = in.readLine();
			if (s != null) {
				String bytes = Codec.hexEncode(s);
				System.out.print("Hex encoded (String): ");
				System.out.println(bytes);
				System.out.print("Hex encoded (byte[]): ");
				System.out.println(Codec.hexEncode(s.getBytes()));
				System.out.print("Re-decoded (byte[]):  ");
				System.out.println(new String(Codec.hexDecode(bytes)));
			}
		} while (s != null );
	}
}


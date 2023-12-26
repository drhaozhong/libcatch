package org.apache.lucene.demo.html;


import java.util.Hashtable;


public class Entities {
	static final Hashtable decoder = new Hashtable(300);

	static final String[] encoder = new String[256];

	static final String decode(String entity) {
		if ((entity.charAt(((entity.length()) - 1))) == ';')
			entity = entity.substring(0, ((entity.length()) - 1));

		if ((entity.charAt(1)) == '#') {
			int start = 2;
			int radix = 10;
			if (((entity.charAt(2)) == 'X') || ((entity.charAt(2)) == 'x')) {
				start++;
				radix = 16;
			}
			Character c = new Character(((char) (Integer.parseInt(entity.substring(start), radix))));
			return c.toString();
		}else {
			String s = ((String) (Entities.decoder.get(entity)));
			if (s != null)
				return s;
			else
				return "";

		}
	}

	public static final String encode(String s) {
		int length = s.length();
		StringBuffer buffer = new StringBuffer((length * 2));
		for (int i = 0; i < length; i++) {
			char c = s.charAt(i);
			int j = ((int) (c));
			if ((j < 256) && ((Entities.encoder[j]) != null)) {
				buffer.append(Entities.encoder[j]);
				buffer.append(';');
			}else
				if (j < 128) {
					buffer.append(c);
				}else {
					buffer.append("&#");
					buffer.append(((int) (c)));
					buffer.append(';');
				}

		}
		return buffer.toString();
	}

	static final void add(String entity, int value) {
		Entities.decoder.put(entity, new Character(((char) (value))).toString());
		if (value < 256)
			Entities.encoder[value] = entity;

	}

	static {
		Entities.add("&nbsp", 160);
		Entities.add("&iexcl", 161);
		Entities.add("&cent", 162);
		Entities.add("&pound", 163);
		Entities.add("&curren", 164);
		Entities.add("&yen", 165);
		Entities.add("&brvbar", 166);
		Entities.add("&sect", 167);
		Entities.add("&uml", 168);
		Entities.add("&copy", 169);
		Entities.add("&ordf", 170);
		Entities.add("&laquo", 171);
		Entities.add("&not", 172);
		Entities.add("&shy", 173);
		Entities.add("&reg", 174);
		Entities.add("&macr", 175);
		Entities.add("&deg", 176);
		Entities.add("&plusmn", 177);
		Entities.add("&sup2", 178);
		Entities.add("&sup3", 179);
		Entities.add("&acute", 180);
		Entities.add("&micro", 181);
		Entities.add("&para", 182);
		Entities.add("&middot", 183);
		Entities.add("&cedil", 184);
		Entities.add("&sup1", 185);
		Entities.add("&ordm", 186);
		Entities.add("&raquo", 187);
		Entities.add("&frac14", 188);
		Entities.add("&frac12", 189);
		Entities.add("&frac34", 190);
		Entities.add("&iquest", 191);
		Entities.add("&Agrave", 192);
		Entities.add("&Aacute", 193);
		Entities.add("&Acirc", 194);
		Entities.add("&Atilde", 195);
		Entities.add("&Auml", 196);
		Entities.add("&Aring", 197);
		Entities.add("&AElig", 198);
		Entities.add("&Ccedil", 199);
		Entities.add("&Egrave", 200);
		Entities.add("&Eacute", 201);
		Entities.add("&Ecirc", 202);
		Entities.add("&Euml", 203);
		Entities.add("&Igrave", 204);
		Entities.add("&Iacute", 205);
		Entities.add("&Icirc", 206);
		Entities.add("&Iuml", 207);
		Entities.add("&ETH", 208);
		Entities.add("&Ntilde", 209);
		Entities.add("&Ograve", 210);
		Entities.add("&Oacute", 211);
		Entities.add("&Ocirc", 212);
		Entities.add("&Otilde", 213);
		Entities.add("&Ouml", 214);
		Entities.add("&times", 215);
		Entities.add("&Oslash", 216);
		Entities.add("&Ugrave", 217);
		Entities.add("&Uacute", 218);
		Entities.add("&Ucirc", 219);
		Entities.add("&Uuml", 220);
		Entities.add("&Yacute", 221);
		Entities.add("&THORN", 222);
		Entities.add("&szlig", 223);
		Entities.add("&agrave", 224);
		Entities.add("&aacute", 225);
		Entities.add("&acirc", 226);
		Entities.add("&atilde", 227);
		Entities.add("&auml", 228);
		Entities.add("&aring", 229);
		Entities.add("&aelig", 230);
		Entities.add("&ccedil", 231);
		Entities.add("&egrave", 232);
		Entities.add("&eacute", 233);
		Entities.add("&ecirc", 234);
		Entities.add("&euml", 235);
		Entities.add("&igrave", 236);
		Entities.add("&iacute", 237);
		Entities.add("&icirc", 238);
		Entities.add("&iuml", 239);
		Entities.add("&eth", 240);
		Entities.add("&ntilde", 241);
		Entities.add("&ograve", 242);
		Entities.add("&oacute", 243);
		Entities.add("&ocirc", 244);
		Entities.add("&otilde", 245);
		Entities.add("&ouml", 246);
		Entities.add("&divide", 247);
		Entities.add("&oslash", 248);
		Entities.add("&ugrave", 249);
		Entities.add("&uacute", 250);
		Entities.add("&ucirc", 251);
		Entities.add("&uuml", 252);
		Entities.add("&yacute", 253);
		Entities.add("&thorn", 254);
		Entities.add("&yuml", 255);
		Entities.add("&fnof", 402);
		Entities.add("&Alpha", 913);
		Entities.add("&Beta", 914);
		Entities.add("&Gamma", 915);
		Entities.add("&Delta", 916);
		Entities.add("&Epsilon", 917);
		Entities.add("&Zeta", 918);
		Entities.add("&Eta", 919);
		Entities.add("&Theta", 920);
		Entities.add("&Iota", 921);
		Entities.add("&Kappa", 922);
		Entities.add("&Lambda", 923);
		Entities.add("&Mu", 924);
		Entities.add("&Nu", 925);
		Entities.add("&Xi", 926);
		Entities.add("&Omicron", 927);
		Entities.add("&Pi", 928);
		Entities.add("&Rho", 929);
		Entities.add("&Sigma", 931);
		Entities.add("&Tau", 932);
		Entities.add("&Upsilon", 933);
		Entities.add("&Phi", 934);
		Entities.add("&Chi", 935);
		Entities.add("&Psi", 936);
		Entities.add("&Omega", 937);
		Entities.add("&alpha", 945);
		Entities.add("&beta", 946);
		Entities.add("&gamma", 947);
		Entities.add("&delta", 948);
		Entities.add("&epsilon", 949);
		Entities.add("&zeta", 950);
		Entities.add("&eta", 951);
		Entities.add("&theta", 952);
		Entities.add("&iota", 953);
		Entities.add("&kappa", 954);
		Entities.add("&lambda", 955);
		Entities.add("&mu", 956);
		Entities.add("&nu", 957);
		Entities.add("&xi", 958);
		Entities.add("&omicron", 959);
		Entities.add("&pi", 960);
		Entities.add("&rho", 961);
		Entities.add("&sigmaf", 962);
		Entities.add("&sigma", 963);
		Entities.add("&tau", 964);
		Entities.add("&upsilon", 965);
		Entities.add("&phi", 966);
		Entities.add("&chi", 967);
		Entities.add("&psi", 968);
		Entities.add("&omega", 969);
		Entities.add("&thetasym", 977);
		Entities.add("&upsih", 978);
		Entities.add("&piv", 982);
		Entities.add("&bull", 8226);
		Entities.add("&hellip", 8230);
		Entities.add("&prime", 8242);
		Entities.add("&Prime", 8243);
		Entities.add("&oline", 8254);
		Entities.add("&frasl", 8260);
		Entities.add("&weierp", 8472);
		Entities.add("&image", 8465);
		Entities.add("&real", 8476);
		Entities.add("&trade", 8482);
		Entities.add("&alefsym", 8501);
		Entities.add("&larr", 8592);
		Entities.add("&uarr", 8593);
		Entities.add("&rarr", 8594);
		Entities.add("&darr", 8595);
		Entities.add("&harr", 8596);
		Entities.add("&crarr", 8629);
		Entities.add("&lArr", 8656);
		Entities.add("&uArr", 8657);
		Entities.add("&rArr", 8658);
		Entities.add("&dArr", 8659);
		Entities.add("&hArr", 8660);
		Entities.add("&forall", 8704);
		Entities.add("&part", 8706);
		Entities.add("&exist", 8707);
		Entities.add("&empty", 8709);
		Entities.add("&nabla", 8711);
		Entities.add("&isin", 8712);
		Entities.add("&notin", 8713);
		Entities.add("&ni", 8715);
		Entities.add("&prod", 8719);
		Entities.add("&sum", 8721);
		Entities.add("&minus", 8722);
		Entities.add("&lowast", 8727);
		Entities.add("&radic", 8730);
		Entities.add("&prop", 8733);
		Entities.add("&infin", 8734);
		Entities.add("&ang", 8736);
		Entities.add("&and", 8743);
		Entities.add("&or", 8744);
		Entities.add("&cap", 8745);
		Entities.add("&cup", 8746);
		Entities.add("&int", 8747);
		Entities.add("&there4", 8756);
		Entities.add("&sim", 8764);
		Entities.add("&cong", 8773);
		Entities.add("&asymp", 8776);
		Entities.add("&ne", 8800);
		Entities.add("&equiv", 8801);
		Entities.add("&le", 8804);
		Entities.add("&ge", 8805);
		Entities.add("&sub", 8834);
		Entities.add("&sup", 8835);
		Entities.add("&nsub", 8836);
		Entities.add("&sube", 8838);
		Entities.add("&supe", 8839);
		Entities.add("&oplus", 8853);
		Entities.add("&otimes", 8855);
		Entities.add("&perp", 8869);
		Entities.add("&sdot", 8901);
		Entities.add("&lceil", 8968);
		Entities.add("&rceil", 8969);
		Entities.add("&lfloor", 8970);
		Entities.add("&rfloor", 8971);
		Entities.add("&lang", 9001);
		Entities.add("&rang", 9002);
		Entities.add("&loz", 9674);
		Entities.add("&spades", 9824);
		Entities.add("&clubs", 9827);
		Entities.add("&hearts", 9829);
		Entities.add("&diams", 9830);
		Entities.add("&quot", 34);
		Entities.add("&amp", 38);
		Entities.add("&lt", 60);
		Entities.add("&gt", 62);
		Entities.add("&OElig", 338);
		Entities.add("&oelig", 339);
		Entities.add("&Scaron", 352);
		Entities.add("&scaron", 353);
		Entities.add("&Yuml", 376);
		Entities.add("&circ", 710);
		Entities.add("&tilde", 732);
		Entities.add("&ensp", 8194);
		Entities.add("&emsp", 8195);
		Entities.add("&thinsp", 8201);
		Entities.add("&zwnj", 8204);
		Entities.add("&zwj", 8205);
		Entities.add("&lrm", 8206);
		Entities.add("&rlm", 8207);
		Entities.add("&ndash", 8211);
		Entities.add("&mdash", 8212);
		Entities.add("&lsquo", 8216);
		Entities.add("&rsquo", 8217);
		Entities.add("&sbquo", 8218);
		Entities.add("&ldquo", 8220);
		Entities.add("&rdquo", 8221);
		Entities.add("&bdquo", 8222);
		Entities.add("&dagger", 8224);
		Entities.add("&Dagger", 8225);
		Entities.add("&permil", 8240);
		Entities.add("&lsaquo", 8249);
		Entities.add("&rsaquo", 8250);
		Entities.add("&euro", 8364);
	}
}


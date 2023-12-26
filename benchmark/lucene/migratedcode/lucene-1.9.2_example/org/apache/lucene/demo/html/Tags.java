package org.apache.lucene.demo.html;


import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


public final class Tags {
	public static final Set WS_ELEMS = Collections.synchronizedSet(new HashSet());

	static {
		Tags.WS_ELEMS.add("<hr");
		Tags.WS_ELEMS.add("<hr/");
		Tags.WS_ELEMS.add("<br");
		Tags.WS_ELEMS.add("<br/");
		Tags.WS_ELEMS.add("<p");
		Tags.WS_ELEMS.add("</p");
		Tags.WS_ELEMS.add("<div");
		Tags.WS_ELEMS.add("</div");
		Tags.WS_ELEMS.add("<td");
		Tags.WS_ELEMS.add("</td");
		Tags.WS_ELEMS.add("<li");
		Tags.WS_ELEMS.add("</li");
		Tags.WS_ELEMS.add("<q");
		Tags.WS_ELEMS.add("</q");
		Tags.WS_ELEMS.add("<blockquote");
		Tags.WS_ELEMS.add("</blockquote");
		Tags.WS_ELEMS.add("<dt");
		Tags.WS_ELEMS.add("</dt");
		Tags.WS_ELEMS.add("<h1");
		Tags.WS_ELEMS.add("</h1");
		Tags.WS_ELEMS.add("<h2");
		Tags.WS_ELEMS.add("</h2");
		Tags.WS_ELEMS.add("<h3");
		Tags.WS_ELEMS.add("</h3");
		Tags.WS_ELEMS.add("<h4");
		Tags.WS_ELEMS.add("</h4");
		Tags.WS_ELEMS.add("<h5");
		Tags.WS_ELEMS.add("</h5");
		Tags.WS_ELEMS.add("<h6");
		Tags.WS_ELEMS.add("</h6");
	}
}


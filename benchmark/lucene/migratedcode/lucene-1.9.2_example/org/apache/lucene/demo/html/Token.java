package org.apache.lucene.demo.html;


public class Token {
	public int kind;

	public int beginLine;

	public int beginColumn;

	public int endLine;

	public int endColumn;

	public String image;

	public Token next;

	public Token specialToken;

	public String toString() {
		return image;
	}

	public static final Token newToken(int ofKind) {
		switch (ofKind) {
			default :
				return new Token();
		}
	}
}


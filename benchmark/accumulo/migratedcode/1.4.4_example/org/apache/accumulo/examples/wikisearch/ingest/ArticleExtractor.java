package org.apache.accumulo.examples.wikisearch.ingest;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.accumulo.examples.wikisearch.normalizer.LcNoDiacriticsNormalizer;
import org.apache.accumulo.examples.wikisearch.normalizer.NumberNormalizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class ArticleExtractor {
	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'Z");

	private static NumberNormalizer nn = new NumberNormalizer();

	private static LcNoDiacriticsNormalizer lcdn = new LcNoDiacriticsNormalizer();

	public static class Article implements Writable {
		int id;

		String title;

		long timestamp;

		String comments;

		String text;

		public Article() {
		}

		private Article(int id, String title, long timestamp, String comments, String text) {
			super();
			this.id = id;
			this.title = title;
			this.timestamp = timestamp;
			this.comments = comments;
			this.text = text;
		}

		public int getId() {
			return id;
		}

		public String getTitle() {
			return title;
		}

		public String getComments() {
			return comments;
		}

		public String getText() {
			return text;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public Map<String, Object> getFieldValues() {
			Map<String, Object> fields = new HashMap<String, Object>();
			fields.put("ID", this.id);
			fields.put("TITLE", this.title);
			fields.put("TIMESTAMP", this.timestamp);
			fields.put("COMMENTS", this.comments);
			return fields;
		}

		public Map<String, String> getNormalizedFieldValues() {
			Map<String, String> fields = new HashMap<String, String>();
			fields.put("ID", ArticleExtractor.nn.normalizeFieldValue("ID", this.id));
			fields.put("TITLE", ArticleExtractor.lcdn.normalizeFieldValue("TITLE", this.title));
			fields.put("TIMESTAMP", ArticleExtractor.nn.normalizeFieldValue("TIMESTAMP", this.timestamp));
			fields.put("COMMENTS", ArticleExtractor.lcdn.normalizeFieldValue("COMMENTS", this.comments));
			return fields;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			id = in.readInt();
			Text foo = new Text();
			foo.readFields(in);
			title = foo.toString();
			timestamp = in.readLong();
			foo.readFields(in);
			comments = foo.toString();
			foo.readFields(in);
			text = foo.toString();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(id);
			new Text(title).write(out);
			out.writeLong(timestamp);
			new Text(comments).write(out);
			new Text(text).write(out);
		}
	}

	public ArticleExtractor() {
	}

	private static XMLInputFactory xmlif = XMLInputFactory.newInstance();

	static {
		ArticleExtractor.xmlif.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, Boolean.TRUE);
	}

	public ArticleExtractor.Article extract(Reader reader) {
		XMLStreamReader xmlr = null;
		try {
			xmlr = ArticleExtractor.xmlif.createXMLStreamReader(reader);
		} catch (XMLStreamException e1) {
			throw new RuntimeException(e1);
		}
		QName titleName = QName.valueOf("title");
		QName textName = QName.valueOf("text");
		QName revisionName = QName.valueOf("revision");
		QName timestampName = QName.valueOf("timestamp");
		QName commentName = QName.valueOf("comment");
		QName idName = QName.valueOf("id");
		Map<QName, StringBuilder> tags = new HashMap<QName, StringBuilder>();
		for (QName tag : new QName[]{ titleName, textName, timestampName, commentName, idName }) {
			tags.put(tag, new StringBuilder());
		}
		StringBuilder articleText = tags.get(textName);
		StringBuilder titleText = tags.get(titleName);
		StringBuilder timestampText = tags.get(timestampName);
		StringBuilder commentText = tags.get(commentName);
		StringBuilder idText = tags.get(idName);
		StringBuilder current = null;
		boolean inRevision = false;
		while (true) {
			try {
				if (!(xmlr.hasNext()))
					break;

				xmlr.next();
			} catch (XMLStreamException e) {
				throw new RuntimeException(e);
			}
			QName currentName = null;
			if (xmlr.hasName()) {
				currentName = xmlr.getName();
			}
			if ((xmlr.isStartElement()) && (tags.containsKey(currentName))) {
				if ((!inRevision) || ((!(currentName.equals(revisionName))) && (!(currentName.equals(idName))))) {
					current = tags.get(currentName);
					current.setLength(0);
				}
			}else
				if ((xmlr.isStartElement()) && (currentName.equals(revisionName))) {
					inRevision = true;
				}else
					if ((xmlr.isEndElement()) && (currentName.equals(revisionName))) {
						inRevision = false;
					}else
						if ((xmlr.isEndElement()) && (current != null)) {
							if (textName.equals(currentName)) {
								String title = titleText.toString();
								String text = articleText.toString();
								String comment = commentText.toString();
								int id = Integer.parseInt(idText.toString());
								long timestamp;
								try {
									timestamp = ArticleExtractor.dateFormat.parse(timestampText.append("+0000").toString()).getTime();
									return new ArticleExtractor.Article(id, title, timestamp, comment, text);
								} catch (ParseException e) {
									return null;
								}
							}
							current = null;
						}else
							if ((current != null) && (xmlr.hasText())) {
								current.append(xmlr.getText());
							}




		} 
		return null;
	}
}


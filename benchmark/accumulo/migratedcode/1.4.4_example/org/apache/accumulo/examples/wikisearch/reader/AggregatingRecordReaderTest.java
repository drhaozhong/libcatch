package org.apache.accumulo.examples.wikisearch.reader;


import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.net.URI;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import org.apache.accumulo.examples.wikisearch.ingest.WikipediaInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Assert;
import org.w3c.dom.Document;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;


public class AggregatingRecordReaderTest {
	public static class MyErrorHandler implements ErrorHandler {
		@Override
		public void error(SAXParseException exception) throws SAXException {
		}

		@Override
		public void fatalError(SAXParseException exception) throws SAXException {
		}

		@Override
		public void warning(SAXParseException exception) throws SAXException {
		}
	}

	private static final String xml1 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + ((((((((((((("<doc>\n" + "  <a>A</a>\n") + "  <b>B</b>\n") + "</doc>\n") + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>") + "<doc>\n") + "  <a>C</a>\n") + "  <b>D</b>\n") + "</doc>\n") + "<?xml version=\"1.0\" encoding=\"UTF-8\"?>") + "<doc>\n") + "  <a>E</a>\n") + "  <b>F</b>\n") + "</doc>\n");

	private static final String xml2 = "  <b>B</b>\n" + (((((((("</doc>\n" + "<doc>\n") + "  <a>C</a>\n") + "  <b>D</b>\n") + "</doc>\n") + "<doc>\n") + "  <a>E</a>\n") + "  <b>F</b>\n") + "</doc>\n");

	private static final String xml3 = "<doc>\n" + (((((((("  <a>A</a>\n" + "  <b>B</b>\n") + "</doc>\n") + "<doc>\n") + "  <a>C</a>\n") + "  <b>D</b>\n") + "</doc>\n") + "<doc>\n") + "  <a>E</a>\n");

	private static final String xml4 = "<doc>" + (((((((((("  <a>A</a>" + "  <b>B</b>") + "</doc>") + "<doc>") + "  <a>C</a>") + "  <b>D</b>") + "</doc>") + "<doc>") + "  <a>E</a>") + "  <b>F</b>") + "</doc>");

	private static final String xml5 = "<doc attr=\"G\">" + (((((((((((("  <a>A</a>" + "  <b>B</b>") + "</doc>") + "<doc>") + "  <a>C</a>") + "  <b>D</b>") + "</doc>") + "<doc attr=\"H\"/>") + "<doc>") + "  <a>E</a>") + "  <b>F</b>") + "</doc>") + "<doc attr=\"I\"/>");

	private Configuration conf = null;

	private TaskAttemptContext ctx = null;

	private static DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

	private XPathFactory xpFactory = XPathFactory.newInstance();

	private XPathExpression EXPR_A = null;

	private XPathExpression EXPR_B = null;

	private XPathExpression EXPR_ATTR = null;

	@org.junit.Before
	public void setUp() throws Exception {
		conf = new Configuration();
		conf.set(AggregatingRecordReader.START_TOKEN, "<doc");
		conf.set(AggregatingRecordReader.END_TOKEN, "</doc>");
		conf.set(AggregatingRecordReader.RETURN_PARTIAL_MATCHES, Boolean.toString(true));
		ctx = new TaskAttemptContext(conf, new TaskAttemptID());
		XPath xp = xpFactory.newXPath();
		EXPR_A = xp.compile("/doc/a");
		EXPR_B = xp.compile("/doc/b");
		EXPR_ATTR = xp.compile("/doc/@attr");
	}

	public File createFile(String data) throws Exception {
		File f = File.createTempFile("aggReaderTest", ".xml");
		f.deleteOnExit();
		FileWriter writer = new FileWriter(f);
		writer.write(data);
		writer.flush();
		writer.close();
		return f;
	}

	private void testXML(Text xml, String aValue, String bValue, String attrValue) throws Exception {
		StringReader reader = new StringReader(xml.toString());
		InputSource source = new InputSource(reader);
		DocumentBuilder parser = AggregatingRecordReaderTest.factory.newDocumentBuilder();
		parser.setErrorHandler(new AggregatingRecordReaderTest.MyErrorHandler());
		Document root = parser.parse(source);
		Assert.assertNotNull(root);
		reader = new StringReader(xml.toString());
		source = new InputSource(reader);
		Assert.assertEquals(EXPR_A.evaluate(source), aValue);
		reader = new StringReader(xml.toString());
		source = new InputSource(reader);
		Assert.assertEquals(EXPR_B.evaluate(source), bValue);
		reader = new StringReader(xml.toString());
		source = new InputSource(reader);
		Assert.assertEquals(EXPR_ATTR.evaluate(source), attrValue);
	}

	@org.junit.Test
	public void testIncorrectArgs() throws Exception {
		File f = createFile(AggregatingRecordReaderTest.xml1);
		Path p = new Path(f.toURI().toString());
		WikipediaInputFormat.WikipediaInputSplit split = new WikipediaInputFormat.WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
		AggregatingRecordReader reader = new AggregatingRecordReader();
		try {
			conf.set(AggregatingRecordReader.START_TOKEN, null);
			conf.set(AggregatingRecordReader.END_TOKEN, null);
			reader.initialize(split, ctx);
			Assert.fail();
		} catch (Exception e) {
			f = null;
		}
		reader.close();
	}

	@org.junit.Test
	public void testCorrectXML() throws Exception {
		File f = createFile(AggregatingRecordReaderTest.xml1);
		Path p = new Path(f.toURI().toString());
		WikipediaInputFormat.WikipediaInputSplit split = new WikipediaInputFormat.WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
		AggregatingRecordReader reader = new AggregatingRecordReader();
		reader.initialize(split, ctx);
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "A", "B", "");
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "C", "D", "");
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "E", "F", "");
		Assert.assertTrue((!(reader.nextKeyValue())));
	}

	@org.junit.Test
	public void testPartialXML() throws Exception {
		File f = createFile(AggregatingRecordReaderTest.xml2);
		Path p = new Path(f.toURI().toString());
		WikipediaInputFormat.WikipediaInputSplit split = new WikipediaInputFormat.WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
		AggregatingRecordReader reader = new AggregatingRecordReader();
		reader.initialize(split, ctx);
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "C", "D", "");
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "E", "F", "");
		Assert.assertTrue((!(reader.nextKeyValue())));
	}

	public void testPartialXML2WithNoPartialRecordsReturned() throws Exception {
		conf.set(AggregatingRecordReader.RETURN_PARTIAL_MATCHES, Boolean.toString(false));
		File f = createFile(AggregatingRecordReaderTest.xml3);
		Path p = new Path(f.toURI().toString());
		WikipediaInputFormat.WikipediaInputSplit split = new WikipediaInputFormat.WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
		AggregatingRecordReader reader = new AggregatingRecordReader();
		reader.initialize(split, ctx);
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "A", "B", "");
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "C", "D", "");
		Assert.assertTrue((!(reader.nextKeyValue())));
	}

	@org.junit.Test
	public void testPartialXML2() throws Exception {
		File f = createFile(AggregatingRecordReaderTest.xml3);
		Path p = new Path(f.toURI().toString());
		WikipediaInputFormat.WikipediaInputSplit split = new WikipediaInputFormat.WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
		AggregatingRecordReader reader = new AggregatingRecordReader();
		reader.initialize(split, ctx);
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "A", "B", "");
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "C", "D", "");
		Assert.assertTrue(reader.nextKeyValue());
		try {
			testXML(reader.getCurrentValue(), "E", "", "");
			Assert.fail("Fragment returned, and it somehow passed XML parsing.");
		} catch (SAXParseException e) {
		}
		Assert.assertTrue((!(reader.nextKeyValue())));
	}

	@org.junit.Test
	public void testLineSplitting() throws Exception {
		File f = createFile(AggregatingRecordReaderTest.xml4);
		Path p = new Path(f.toURI().toString());
		WikipediaInputFormat.WikipediaInputSplit split = new WikipediaInputFormat.WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
		AggregatingRecordReader reader = new AggregatingRecordReader();
		reader.initialize(split, ctx);
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "A", "B", "");
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "C", "D", "");
		Assert.assertTrue(reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "E", "F", "");
		Assert.assertTrue((!(reader.nextKeyValue())));
	}

	@org.junit.Test
	public void testNoEndTokenHandling() throws Exception {
		File f = createFile(AggregatingRecordReaderTest.xml5);
		Path p = new Path(f.toURI().toString());
		WikipediaInputFormat.WikipediaInputSplit split = new WikipediaInputFormat.WikipediaInputSplit(new FileSplit(p, 0, f.length(), null), 0);
		AggregatingRecordReader reader = new AggregatingRecordReader();
		reader.initialize(split, ctx);
		Assert.assertTrue("Not enough records returned.", reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "A", "B", "G");
		Assert.assertTrue("Not enough records returned.", reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "C", "D", "");
		Assert.assertTrue("Not enough records returned.", reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "", "", "H");
		Assert.assertTrue("Not enough records returned.", reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "E", "F", "");
		Assert.assertTrue("Not enough records returned.", reader.nextKeyValue());
		testXML(reader.getCurrentValue(), "", "", "I");
		Assert.assertTrue("Too many records returned.", (!(reader.nextKeyValue())));
	}
}


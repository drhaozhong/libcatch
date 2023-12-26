package org.apache.lucene.xmlparser.webdemo;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;
import javax.servlet.GenericServlet;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.ext.Extensions;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.queryparser.xml.QueryTemplateManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import static org.apache.lucene.document.Field.Index.ANALYZED;
import static org.apache.lucene.document.Field.Index.ANALYZED_NO_NORMS;
import static org.apache.lucene.document.Field.Store.YES;


public class FormBasedXmlQueryDemo extends HttpServlet {
	private QueryTemplateManager queryTemplateManager;

	private Extensions.Pair xmlParser;

	private IndexSearcher searcher;

	private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		try {
			openExampleIndex();
			String xslFile = config.getInitParameter("xslFile");
			String defaultStandardQueryParserField = config.getInitParameter("defaultStandardQueryParserField");
			queryTemplateManager = new QueryTemplateManager(getServletContext().getResourceAsStream(("/WEB-INF/" + xslFile)));
			xmlParser = new Extensions.Pair(defaultStandardQueryParserField, analyzer);
		} catch (Exception e) {
			throw new ServletException("Error loading query template", e);
		}
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		Properties completedFormFields = new Properties();
		Enumeration pNames = request.getParameterNames();
		while (pNames.hasMoreElements()) {
			String propName = ((String) (pNames.nextElement()));
			String value = request.getParameter(propName);
			if ((value != null) && ((value.trim().length()) > 0)) {
				completedFormFields.setProperty(propName, value);
			}
		} 
		try {
			Document xmlQuery = queryTemplateManager.getQueryAsDOM(completedFormFields);
			Query query = this.xmlParser.getQuery(xmlQuery.getDocumentElement());
			TopDocs topDocs = searcher.search(query, 10);
			if (topDocs != null) {
				ScoreDoc[] sd = topDocs.scoreDocs;
				org.apache.lucene.document.Document[] results = new org.apache.lucene.document.Document[sd.length];
				for (int i = 0; i < (results.length); i++) {
					results[i] = searcher.doc(sd[i].doc);
					request.setAttribute("results", results);
				}
			}
			RequestDispatcher dispatcher = getServletContext().getRequestDispatcher("/index.jsp");
			dispatcher.forward(request, response);
		} catch (Exception e) {
			throw new ServletException("Error processing query", e);
		}
	}

	private void openExampleIndex() throws IOException, CorruptIndexException {
		RAMDirectory rd = new RAMDirectory();
		IndexWriter writer = new IndexWriter(rd, analyzer, MaxFieldLength);
		InputStream dataIn = getServletContext().getResourceAsStream("/WEB-INF/data.tsv");
		BufferedReader br = new BufferedReader(new InputStreamReader(dataIn));
		String line = br.readLine();
		while (line != null) {
			line = line.trim();
			if ((line.length()) > 0) {
				StringTokenizer st = new StringTokenizer(line, "\t");
				org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
				doc.add(new Field("location", st.nextToken(), YES, ANALYZED_NO_NORMS));
				doc.add(new Field("salary", st.nextToken(), YES, ANALYZED_NO_NORMS));
				doc.add(new Field("type", st.nextToken(), YES, ANALYZED_NO_NORMS));
				doc.add(new Field("description", st.nextToken(), YES, ANALYZED));
				writer.addDocument(doc);
			}
			line = br.readLine();
		} 
		writer.close();
		searcher = new IndexSearcher(rd, true);
	}
}


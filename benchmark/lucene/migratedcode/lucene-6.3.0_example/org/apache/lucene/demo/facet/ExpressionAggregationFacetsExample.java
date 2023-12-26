package org.apache.lucene.demo.facet;


import java.io.IOException;
import java.io.PrintStream;
import java.text.ParseException;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetSumValueSource;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import static org.apache.lucene.document.Field.Store.NO;
import static org.apache.lucene.index.IndexWriterConfig.OpenMode.CREATE;
import static org.apache.lucene.search.SortField.Type.LONG;
import static org.apache.lucene.search.SortField.Type.SCORE;


public class ExpressionAggregationFacetsExample {
	private final Directory indexDir = new RAMDirectory();

	private final Directory taxoDir = new RAMDirectory();

	private final FacetsConfig config = new FacetsConfig();

	public ExpressionAggregationFacetsExample() {
	}

	private void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(CREATE));
		DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
		Document doc = new Document();
		doc.add(new TextField("c", "foo bar", NO));
		doc.add(new NumericDocValuesField("popularity", 5L));
		doc.add(new FacetField("A", "B"));
		indexWriter.addDocument(config.build(taxoWriter, doc));
		doc = new Document();
		doc.add(new TextField("c", "foo foo bar", NO));
		doc.add(new NumericDocValuesField("popularity", 3L));
		doc.add(new FacetField("A", "C"));
		indexWriter.addDocument(config.build(taxoWriter, doc));
		indexWriter.close();
		taxoWriter.close();
	}

	private FacetResult search() throws IOException, ParseException {
		DirectoryReader indexReader = DirectoryReader.open(indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
		Expression expr = JavascriptCompiler.compile("_score * sqrt(popularity)");
		SimpleBindings bindings = new SimpleBindings();
		bindings.add(new SortField("_score", SCORE));
		bindings.add(new SortField("popularity", LONG));
		FacetsCollector fc = new FacetsCollector(true);
		FacetsCollector.search(searcher, new MatchAllDocsQuery(), 10, fc);
		Facets facets = new TaxonomyFacetSumValueSource(taxoReader, config, fc, getValueSource(bindings));
		FacetResult result = facets.getTopChildren(10, "A");
		indexReader.close();
		taxoReader.close();
		return result;
	}

	public FacetResult runSearch() throws IOException, ParseException {
		index();
		return search();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Facet counting example:");
		System.out.println("-----------------------");
		FacetResult result = new ExpressionAggregationFacetsExample().runSearch();
		System.out.println(result);
	}
}


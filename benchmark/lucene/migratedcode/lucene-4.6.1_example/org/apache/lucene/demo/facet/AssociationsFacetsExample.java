package org.apache.lucene.demo.facet;


import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.associations.CategoryAssociation;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.FloatAssociationFacetField;
import org.apache.lucene.facet.taxonomy.IntAssociationFacetField;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetSumFloatAssociations;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.CachingCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.suggest.Lookup.LookupPriorityQueue;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;


public class AssociationsFacetsExample {
	public static CategoryPath[][] CATEGORIES = new CategoryPath[][]{ new CategoryPath[]{ new CategoryPath("tags", "lucene"), new CategoryPath("genre", "computing") }, new CategoryPath[]{ new CategoryPath("tags", "lucene"), new CategoryPath("tags", "solr"), new CategoryPath("genre", "computing"), new CategoryPath("genre", "software") } };

	public static CategoryAssociation[][] ASSOCIATIONS = new CategoryAssociation[][]{ new CategoryAssociation[]{ new CategoryPath(3), new TaxonomyFacetSumFloatAssociations(0.87F) }, new CategoryAssociation[]{ new CategoryPath(), new CategoryPath(), new TaxonomyFacetSumFloatAssociations(0.75F), new TaxonomyFacetSumFloatAssociations(0.34F) } };

	private final Directory indexDir = new RAMDirectory();

	private final Directory taxoDir = new RAMDirectory();

	public AssociationsFacetsExample() {
	}

	private void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));
		DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
		FacetField facetFields = new FloatAssociationFacetField(taxoWriter);
		for (int i = 0; i < (AssociationsFacetsExample.CATEGORIES.length); i++) {
			Document doc = new Document();
			IntAssociationFacetField associations = new IntAssociationFacetField();
			for (int j = 0; j < (AssociationsFacetsExample.CATEGORIES[i].length); j++) {
			}
			indexWriter.addDocument(doc);
		}
		indexWriter.close();
		taxoWriter.close();
	}

	private List<FacetResult> sumAssociations() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(indexDir);
		IndexSearcher searcher = new IndexSearcher(indexReader);
		TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
		CategoryPath tags = new CategoryPath("tags");
		CategoryPath genre = new CategoryPath("genre");
		IndexSearcher fsp = new IndexSearcher(new IntAssociationFacetField(tags, 10), new FloatAssociationFacetField(genre, 10));
		FacetsCollector fc = CachingCollector.create(fsp, indexReader, taxoReader);
		searcher.search(new MatchAllDocsQuery(), fc);
		List<FacetResult> facetResults = getResults();
		indexReader.close();
		taxoReader.close();
		return facetResults;
	}

	public List<FacetResult> runSumAssociations() throws IOException {
		index();
		return sumAssociations();
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Sum associations example:");
		System.out.println("-------------------------");
		List<FacetResult> results = new AssociationsFacetsExample().runSumAssociations();
		for (FacetResult res : results) {
			System.out.println(res);
		}
	}
}


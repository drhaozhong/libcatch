package org.apache.lucene.demo.facet;


import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.associations.AssociationsFacetFields;
import org.apache.lucene.facet.associations.CategoryAssociation;
import org.apache.lucene.facet.associations.CategoryAssociationsContainer;
import org.apache.lucene.facet.associations.CategoryFloatAssociation;
import org.apache.lucene.facet.associations.CategoryIntAssociation;
import org.apache.lucene.facet.associations.SumFloatAssociationFacetsAggregator;
import org.apache.lucene.facet.associations.SumIntAssociationFacetRequest;
import org.apache.lucene.facet.associations.SumIntAssociationFacetsAggregator;
import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.search.FacetResult;
import org.apache.lucene.facet.search.FacetsAccumulator;
import org.apache.lucene.facet.search.FacetsAggregator;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;


public class AssociationsFacetsExample {
	public static CategoryPath[][] CATEGORIES = new CategoryPath[][]{ new CategoryPath[]{ new CategoryPath("tags", "lucene"), new CategoryPath("genre", "computing") }, new CategoryPath[]{ new CategoryPath("tags", "lucene"), new CategoryPath("tags", "solr"), new CategoryPath("genre", "computing"), new CategoryPath("genre", "software") } };

	public static CategoryAssociation[][] ASSOCIATIONS = new CategoryAssociation[][]{ new CategoryAssociation[]{ new CategoryIntAssociation(3), new CategoryFloatAssociation(0.87F) }, new CategoryAssociation[]{ new CategoryIntAssociation(1), new CategoryIntAssociation(2), new CategoryFloatAssociation(0.75F), new CategoryFloatAssociation(0.34F) } };

	private final Directory indexDir = new RAMDirectory();

	private final Directory taxoDir = new RAMDirectory();

	public AssociationsFacetsExample() {
	}

	private void index() throws IOException {
		IndexWriter indexWriter = new IndexWriter(indexDir, new IndexWriterConfig(FacetExamples.EXAMPLES_VER, new WhitespaceAnalyzer(FacetExamples.EXAMPLES_VER)));
		DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir);
		FacetFields facetFields = new AssociationsFacetFields(taxoWriter);
		for (int i = 0; i < (AssociationsFacetsExample.CATEGORIES.length); i++) {
			Document doc = new Document();
			CategoryAssociationsContainer associations = new CategoryAssociationsContainer();
			for (int j = 0; j < (AssociationsFacetsExample.CATEGORIES[i].length); j++) {
				associations.setAssociation(AssociationsFacetsExample.CATEGORIES[i][j], AssociationsFacetsExample.ASSOCIATIONS[i][j]);
			}
			facetFields.addFields(doc, associations);
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
		FacetSearchParams fsp = new FacetSearchParams(new SumIntAssociationFacetRequest(tags, 10), new SumIntAssociationFacetRequest(genre, 10));
		final Map<CategoryPath, FacetsAggregator> aggregators = new HashMap<CategoryPath, FacetsAggregator>();
		aggregators.put(tags, new SumIntAssociationFacetsAggregator());
		aggregators.put(genre, new SumFloatAssociationFacetsAggregator());
		FacetsAccumulator fa = new FacetsAccumulator(fsp) {
			@Override
			public FacetsAggregator getAggregator() {
				return new SumFloatAssociationFacetsAggregator();
			}
		};
		FacetsCollector fc = FacetsCollector.create(fa);
		searcher.search(new MatchAllDocsQuery(), fc);
		List<FacetResult> facetResults = fc.getFacetResults();
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


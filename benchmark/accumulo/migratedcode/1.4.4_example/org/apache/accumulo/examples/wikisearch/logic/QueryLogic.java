package org.apache.accumulo.examples.wikisearch.logic;


import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Formatter;
import java.util.List;
import java.util.Set;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.accumulo.examples.wikisearch.parser.RangeCalculator;
import org.apache.log4j.Logger;


public class QueryLogic extends AbstractQueryLogic {
	protected static Logger log = Logger.getLogger(QueryLogic.class);

	public QueryLogic() {
		super();
	}

	protected Collection<Range> getFullScanRange(Date begin, Date end, Multimap<String, QueryParser.QueryTerm> terms) {
		return Collections.singletonList(new Range());
	}

	public RangeCalculator getTermIndexInformation(Connector para0, Authorizations para1, Multimap<String, Formatter> para2, Multimap<String, QueryParser.QueryTerm> para3, String para4, String para5, String para6, int para7, Set<String> para8) {
		return null;
	}

	public AbstractQueryLogic.IndexRanges getTermIndexInformation(Connector para0, Authorizations para1, String para2, Set<String> para3) {
		return null;
	}
}


package org.apache.accumulo.examples.wikisearch.parser;


import com.google.common.collect.Multimap;
import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.accumulo.examples.wikisearch.function.QueryFunctions;
import org.apache.accumulo.examples.wikisearch.jexl.Arithmetic;
import org.apache.accumulo.examples.wikisearch.parser.EventFields.FieldValue;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class QueryEvaluator {
	private static Logger log = Logger.getLogger(QueryEvaluator.class);

	private static JexlEngine engine = new JexlEngine(null, new Arithmetic(false), null, null);

	static {
		QueryEvaluator.engine.setSilent(false);
		QueryEvaluator.engine.setCache(128);
		Map<String, Object> functions = new HashMap<String, Object>();
		functions.put("f", QueryFunctions.class);
		QueryEvaluator.engine.setFunctions(functions);
	}

	private String query = null;

	private Set<String> literals = null;

	private Multimap<String, QueryParser.QueryTerm> terms = null;

	private String modifiedQuery = null;

	private JexlContext ctx = new MapContext();

	private boolean caseInsensitive = true;

	public QueryEvaluator(String query) throws ParseException {
		this.caseInsensitive = true;
		if (caseInsensitive) {
			query = query.toLowerCase();
		}
		this.query = query;
		QueryParser parser = new QueryParser();
		parser.execute(query);
		this.terms = parser.getQueryTerms();
		if (caseInsensitive) {
			literals = new HashSet<String>();
			for (String lit : parser.getQueryIdentifiers()) {
				literals.add(lit.toLowerCase());
			}
		}else {
			this.literals = parser.getQueryIdentifiers();
		}
	}

	public QueryEvaluator(String query, boolean insensitive) throws ParseException {
		this.caseInsensitive = insensitive;
		if (this.caseInsensitive) {
			query = query.toLowerCase();
		}
		this.query = query;
		QueryParser parser = new QueryParser();
		parser.execute(query);
		this.terms = parser.getQueryTerms();
		if (caseInsensitive) {
			literals = new HashSet<String>();
			for (String lit : parser.getQueryIdentifiers()) {
				literals.add(lit.toLowerCase());
			}
		}else {
			this.literals = parser.getQueryIdentifiers();
		}
	}

	public String getQuery() {
		return this.query;
	}

	public void printLiterals() {
		for (String s : literals) {
			System.out.println(("literal: " + s));
		}
	}

	public void setLevel(Level lev) {
		QueryEvaluator.log.setLevel(lev);
	}

	public StringBuilder rewriteQuery(StringBuilder query, String fieldName, Collection<EventFields.FieldValue> fieldValues) {
		if (QueryEvaluator.log.isDebugEnabled()) {
			QueryEvaluator.log.debug("rewriteQuery");
		}
		if (caseInsensitive) {
			fieldName = fieldName.toLowerCase();
		}
		if (QueryEvaluator.log.isDebugEnabled()) {
			QueryEvaluator.log.debug(("Modifying original query: " + query));
		}
		String[] values = new String[fieldValues.size()];
		int idx = 0;
		for (EventFields.FieldValue fv : fieldValues) {
			if (caseInsensitive) {
				values[idx] = new String(fv.getValue()).toLowerCase();
			}else {
				values[idx] = new String(fv.getValue());
			}
			idx++;
		}
		ctx.set(fieldName, values);
		Collection<QueryParser.QueryTerm> qt = terms.get(fieldName);
		StringBuilder script = new StringBuilder();
		script.append("_").append(fieldName).append(" = false;\n");
		script.append("for (field : ").append(fieldName).append(") {\n");
		for (QueryParser.QueryTerm t : qt) {
			if (!(t.getOperator().equals(JexlOperatorConstants.getOperator(ParserTreeConstants.JJTFUNCTIONNODE)))) {
				script.append("\tif (_").append(fieldName).append(" == false && field ").append(t.getOperator()).append(" ").append(t.getValue()).append(") { \n");
			}else {
				script.append("\tif (_").append(fieldName).append(" == false && ").append(t.getValue().toString().replace(fieldName, "field")).append(") { \n");
			}
			script.append("\t\t_").append(fieldName).append(" = true;\n");
			script.append("\t}\n");
		}
		script.append("}\n");
		query.insert(0, script.toString());
		StringBuilder newPredicate = new StringBuilder();
		newPredicate.append("_").append(fieldName).append(" == true");
		for (QueryParser.QueryTerm t : qt) {
			StringBuilder predicate = new StringBuilder();
			int start = 0;
			if (!(t.getOperator().equals(JexlOperatorConstants.getOperator(ParserTreeConstants.JJTFUNCTIONNODE)))) {
				predicate.append(fieldName).append(" ").append(t.getOperator()).append(" ").append(t.getValue());
				start = query.indexOf(predicate.toString());
			}else {
				predicate.append(t.getValue().toString());
				start = query.indexOf(predicate.toString());
			}
			if ((-1) == start) {
				QueryEvaluator.log.warn(((("Unable to find predicate: " + (predicate.toString())) + " in rewritten query: ") + (query.toString())));
			}
			int length = predicate.length();
			query.replace(start, (start + length), newPredicate.toString());
		}
		if (QueryEvaluator.log.isDebugEnabled()) {
			QueryEvaluator.log.debug(("leaving rewriteQuery with: " + (query.toString())));
		}
		return query;
	}

	public boolean evaluate(EventFields eventFields) {
		this.modifiedQuery = null;
		boolean rewritten = false;
		StringBuilder q = new StringBuilder(query);
		HashSet<String> literalsCopy = new HashSet<String>(literals);
		for (Map.Entry<String, Collection<EventFields.FieldValue>> field : eventFields.asMap().entrySet()) {
			String fName = field.getKey();
			if (caseInsensitive) {
				fName = fName.toLowerCase();
			}
			if (!(literals.contains(fName))) {
				continue;
			}else {
				literalsCopy.remove(fName);
			}
			if ((field.getValue().size()) == 0) {
				continue;
			}else
				if ((field.getValue().size()) == 1) {
					if (caseInsensitive) {
						ctx.set(field.getKey().toLowerCase(), new String(field.getValue().iterator().next().getValue()).toLowerCase());
					}else {
						ctx.set(field.getKey(), new String(field.getValue().iterator().next().getValue()));
					}
				}else {
					q = rewriteQuery(q, field.getKey(), field.getValue());
					rewritten = true;
				}

		}
		for (String lit : literalsCopy) {
			ctx.set(lit, null);
		}
		if (QueryEvaluator.log.isDebugEnabled()) {
			QueryEvaluator.log.debug(("Evaluating query: " + (q.toString())));
		}
		this.modifiedQuery = q.toString();
		Boolean result = null;
		if (rewritten) {
			Script script = QueryEvaluator.engine.createScript(this.modifiedQuery);
			try {
				result = ((Boolean) (script.execute(ctx)));
			} catch (Exception e) {
				QueryEvaluator.log.error(((("Error evaluating script: " + (this.modifiedQuery)) + " against event") + (eventFields.toString())), e);
			}
		}else {
			Expression expr = QueryEvaluator.engine.createExpression(this.modifiedQuery);
			try {
				result = ((Boolean) (expr.evaluate(ctx)));
			} catch (Exception e) {
				QueryEvaluator.log.error(((("Error evaluating expression: " + (this.modifiedQuery)) + " against event") + (eventFields.toString())), e);
			}
		}
		if ((null != result) && result) {
			return true;
		}else {
			return false;
		}
	}

	public String getModifiedQuery() {
		return this.modifiedQuery;
	}
}


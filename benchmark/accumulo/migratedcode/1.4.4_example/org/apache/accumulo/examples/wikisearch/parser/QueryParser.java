package org.apache.accumulo.examples.wikisearch.parser;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.TermResult;
import org.apache.commons.collections.map.AbstractHashedMap;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFloatLiteral;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTIntegerLiteral;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;


public class QueryParser implements ParserVisitor {
	public static class QueryTerm {
		private boolean negated = false;

		private String operator = null;

		private Object value = null;

		public QueryTerm(boolean negated, String operator, Object value) {
			super();
			this.negated = negated;
			this.operator = operator;
			this.value = value;
		}

		public boolean isNegated() {
			return negated;
		}

		public String getOperator() {
			return operator;
		}

		public Object getValue() {
			return value;
		}

		public void setNegated(boolean negated) {
			this.negated = negated;
		}

		public void setOperator(String operator) {
			this.operator = operator;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public String toString() {
			StringBuilder buf = new StringBuilder();
			buf.append("negated: ").append(negated).append(", operator: ").append(operator).append(", value: ").append(value);
			return buf.toString();
		}
	}

	static class ObjectHolder {
		Object object;

		public Object getObject() {
			return object;
		}

		public void setObject(Object object) {
			this.object = object;
		}
	}

	static class FunctionResult {
		private List<QueryParser.TermResult> terms = new ArrayList<QueryParser.TermResult>();

		public List<QueryParser.TermResult> getTerms() {
			return terms;
		}
	}

	static class TermResult {
		Object value;

		public TermResult(Object value) {
			this.value = value;
		}
	}

	static class LiteralResult {
		Object value;

		public LiteralResult(Object value) {
			this.value = value;
		}
	}

	static class EvaluationContext {
		boolean inOrContext = false;

		boolean inNotContext = false;

		boolean inAndContext = false;
	}

	private static class CacheEntry {
		private Set<String> negatedTerms = null;

		private Set<String> andTerms = null;

		private Set<String> orTerms = null;

		private Set<Object> literals = null;

		private Multimap<String, QueryParser.QueryTerm> terms = null;

		private ASTJexlScript rootNode = null;

		private TreeNode tree = null;

		public CacheEntry(Set<String> negatedTerms, Set<String> andTerms, Set<String> orTerms, Set<Object> literals, Multimap<String, QueryParser.QueryTerm> terms, ASTJexlScript rootNode, TreeNode tree) {
			super();
			this.negatedTerms = negatedTerms;
			this.andTerms = andTerms;
			this.orTerms = orTerms;
			this.literals = literals;
			this.terms = terms;
			this.rootNode = rootNode;
			this.tree = tree;
		}

		public Set<String> getNegatedTerms() {
			return negatedTerms;
		}

		public Set<String> getAndTerms() {
			return andTerms;
		}

		public Set<String> getOrTerms() {
			return orTerms;
		}

		public Set<Object> getLiterals() {
			return literals;
		}

		public Multimap<String, QueryParser.QueryTerm> getTerms() {
			return terms;
		}

		public ASTJexlScript getRootNode() {
			return rootNode;
		}

		public TreeNode getTree() {
			return tree;
		}
	}

	private static final int SEED = 650567;

	private static LRUMap cache = new LRUMap();

	protected Set<String> negatedTerms = new HashSet<String>();

	private Set<String> andTerms = new HashSet<String>();

	private Set<String> orTerms = new HashSet<String>();

	private Set<Object> literals = new HashSet<Object>();

	private Multimap<String, QueryParser.QueryTerm> terms = HashMultimap.create();

	private ASTJexlScript rootNode = null;

	private TreeNode tree = null;

	private int hashVal = 0;

	public QueryParser() {
	}

	private void reset() {
		this.negatedTerms.clear();
		this.andTerms.clear();
		this.orTerms.clear();
		this.literals.clear();
		this.terms = HashMultimap.create();
	}

	public void execute(String query) throws ParseException {
		reset();
		query = query.replaceAll("\\s+AND\\s+", " and ");
		query = query.replaceAll("\\s+OR\\s+", " or ");
		query = query.replaceAll("\\s+NOT\\s+", " not ");
		Hash hash = MurmurHash.getInstance();
		this.hashVal = hash.hash(query.getBytes(), QueryParser.SEED);
		QueryParser.CacheEntry entry = null;
		synchronized(QueryParser.cache) {
			entry = ((QueryParser.CacheEntry) (QueryParser.cache.get(hashVal)));
		}
		if (entry != null) {
			this.negatedTerms = entry.getNegatedTerms();
			this.andTerms = entry.getAndTerms();
			this.orTerms = entry.getOrTerms();
			this.literals = entry.getLiterals();
			this.terms = entry.getTerms();
			this.rootNode = entry.getRootNode();
			this.tree = entry.getTree();
		}else {
			Parser p = new Parser(new StringReader(";"));
			rootNode = p.parse(new StringReader(query), null);
			rootNode.childrenAccept(this, null);
			TreeBuilder builder = new TreeBuilder(rootNode);
			tree = builder.getRootNode();
			entry = new QueryParser.CacheEntry(this.negatedTerms, this.andTerms, this.orTerms, this.literals, this.terms, rootNode, tree);
			synchronized(QueryParser.cache) {
				QueryParser.cache.put(hashVal, entry);
			}
		}
	}

	public int getHashValue() {
		return this.hashVal;
	}

	public TreeNode getIteratorTree() {
		return this.tree;
	}

	public ASTJexlScript getAST() {
		return this.rootNode;
	}

	public Set<String> getNegatedTermsForOptimizer() {
		return negatedTerms;
	}

	public Set<String> getAndTermsForOptimizer() {
		return andTerms;
	}

	public Set<String> getOrTermsForOptimizer() {
		return orTerms;
	}

	public Set<Object> getQueryLiterals() {
		return literals;
	}

	public Set<String> getQueryIdentifiers() {
		return terms.keySet();
	}

	public Multimap<String, QueryParser.QueryTerm> getQueryTerms() {
		return terms;
	}

	public Object visit(ASTBlock node, Object data) {
		return null;
	}

	public Object visit(ASTAssignment node, Object data) {
		return null;
	}

	public Object visit(ASTOrNode node, Object data) {
		boolean previouslyInOrContext = false;
		QueryParser.EvaluationContext ctx = null;
		if ((null != data) && (data instanceof QueryParser.EvaluationContext)) {
			ctx = ((QueryParser.EvaluationContext) (data));
			previouslyInOrContext = ctx.inOrContext;
		}else {
			ctx = new QueryParser.EvaluationContext();
		}
		ctx.inOrContext = true;
		node.jjtGetChild(0).jjtAccept(this, ctx);
		node.jjtGetChild(1).jjtAccept(this, ctx);
		if ((null != data) && (!previouslyInOrContext))
			ctx.inOrContext = false;

		return null;
	}

	public Object visit(ASTBitwiseOrNode node, Object data) {
		return null;
	}

	public Object visit(ASTBitwiseXorNode node, Object data) {
		return null;
	}

	public Object visit(ASTNENode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = true;
		if ((null != data) && (data instanceof QueryParser.EvaluationContext)) {
			QueryParser.EvaluationContext ctx = ((QueryParser.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		if (negated)
			negatedTerms.add(fieldName.toString());

		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		terms.put(fieldName.toString(), term);
		return null;
	}

	public Object visit(ASTGENode node, Object data) {
		StringBuilder fieldName = new StringBuilder();
		QueryParser.ObjectHolder value = new QueryParser.ObjectHolder();
		Object left = node.jjtGetChild(0).jjtAccept(this, data);
		Object right = node.jjtGetChild(1).jjtAccept(this, data);
		if ((left instanceof QueryParser.FunctionResult) || (right instanceof QueryParser.FunctionResult))
			return null;

		decodeResults(left, right, fieldName, value);
		boolean negated = false;
		if ((null != data) && (data instanceof QueryParser.EvaluationContext)) {
			QueryParser.EvaluationContext ctx = ((QueryParser.EvaluationContext) (data));
			if (ctx.inNotContext)
				negated = !negated;

		}
		QueryParser.QueryTerm term = new QueryParser.QueryTerm(negated, JexlOperatorConstants.getOperator(node.getClass()), value.getObject());
		terms.put(fieldName.toString(), term);
		return null;
	}

	public Object visit(ASTAdditiveNode node, Object data) {
		return null;
	}

	public Object visit(ASTAdditiveOperator node, Object data) {
		return null;
	}

	public Object visit(ASTMulNode node, Object data) {
		return null;
	}

	public Object visit(ASTUnaryMinusNode node, Object data) {
		return null;
	}

	public Object visit(ASTNotNode node, Object data) {
		boolean previouslyInNotContext = false;
		QueryParser.EvaluationContext ctx = null;
		if ((null != data) && (data instanceof QueryParser.EvaluationContext)) {
			ctx = ((QueryParser.EvaluationContext) (data));
			previouslyInNotContext = ctx.inNotContext;
		}else {
			ctx = new QueryParser.EvaluationContext();
		}
		ctx.inNotContext = true;
		node.jjtGetChild(0).jjtAccept(this, ctx);
		if ((null != data) && (!previouslyInNotContext))
			ctx.inNotContext = false;

		return null;
	}

	public Object visit(ASTIdentifier node, Object data) {
		if (data instanceof QueryParser.EvaluationContext) {
			QueryParser.EvaluationContext ctx = ((QueryParser.EvaluationContext) (data));
			if (ctx.inAndContext)
				andTerms.add(node.image);

			if (ctx.inNotContext)
				negatedTerms.add(node.image);

			if (ctx.inOrContext)
				orTerms.add(node.image);

		}
		return new QueryParser.TermResult(node.image);
	}

	public Object visit(ASTNullLiteral node, Object data) {
		literals.add(node.image);
		return new QueryParser.LiteralResult(node.image);
	}

	public Object visit(ASTFalseNode node, Object data) {
		return new QueryParser.LiteralResult(node.image);
	}

	public Object visit(ASTIntegerLiteral node, Object data) {
		literals.add(node.image);
		return new QueryParser.LiteralResult(node.image);
	}

	public Object visit(ASTFloatLiteral node, Object data) {
		literals.add(node.image);
		return new QueryParser.LiteralResult(node.image);
	}

	public Object visit(ASTStringLiteral node, Object data) {
		literals.add((("'" + (node.image)) + "'"));
		return new QueryParser.LiteralResult((("'" + (node.image)) + "'"));
	}

	public Object visit(ASTMapEntry node, Object data) {
		return null;
	}

	public Object visit(ASTSizeFunction node, Object data) {
		return null;
	}

	public Object visit(ASTSizeMethod node, Object data) {
		return null;
	}

	public Object visit(ASTConstructorNode node, Object data) {
		return null;
	}

	public Object visit(ASTArrayAccess node, Object data) {
		return null;
	}

	protected void decodeResults(Object left, Object right, StringBuilder fieldName, QueryParser.ObjectHolder holder) {
		if (left instanceof QueryParser.TermResult) {
			QueryParser.TermResult tr = ((QueryParser.TermResult) (left));
			fieldName.append(((String) (tr.value)));
			if (right instanceof QueryParser.LiteralResult) {
				holder.setObject(((QueryParser.LiteralResult) (right)).value);
			}else {
				throw new IllegalArgumentException("Object mismatch");
			}
		}else
			if (right instanceof QueryParser.TermResult) {
				QueryParser.TermResult tr = ((QueryParser.TermResult) (right));
				fieldName.append(((String) (tr.value)));
				if (left instanceof QueryParser.LiteralResult) {
					holder.setObject(((QueryParser.LiteralResult) (left)).value);
				}else {
					throw new IllegalArgumentException("Object mismatch");
				}
			}else {
				throw new IllegalArgumentException("No Term specified in query");
			}

	}

	public Object visit(ASTTrueNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTBitwiseComplNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTReference para0, Object para1) {
		return null;
	}

	public Object visit(ASTModNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTERNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTNumberLiteral para0, Object para1) {
		return null;
	}

	public Object visit(ASTEmptyFunction para0, Object para1) {
		return null;
	}

	public Object visit(ASTBitwiseAndNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTReferenceExpression para0, Object para1) {
		return null;
	}

	public Object visit(ASTIfStatement para0, Object para1) {
		return null;
	}

	public Object visit(ASTNRNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTFunctionNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTAmbiguous para0, Object para1) {
		return null;
	}

	public Object visit(ASTArrayLiteral para0, Object para1) {
		return null;
	}

	public Object visit(ASTMethodNode para0, Object para1) {
		return null;
	}

	public Object visit(SimpleNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTJexlScript para0, Object para1) {
		return null;
	}

	public Object visit(ASTWhileStatement para0, Object para1) {
		return null;
	}

	public Object visit(ASTVar para0, Object para1) {
		return null;
	}

	public Object visit(ASTDivNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTGTNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTMapLiteral para0, Object para1) {
		return null;
	}

	public Object visit(ASTForeachStatement para0, Object para1) {
		return null;
	}

	public Object visit(ASTEQNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTAndNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTTernaryNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTLENode para0, Object para1) {
		return null;
	}
}


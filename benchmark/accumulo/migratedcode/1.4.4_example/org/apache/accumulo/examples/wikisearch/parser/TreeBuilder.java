package org.apache.accumulo.examples.wikisearch.parser;


import com.google.common.collect.Multimap;
import java.io.StringReader;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
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
import org.apache.commons.jexl2.parser.ASTLTNode;
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
import org.apache.commons.jexl2.parser.ASTReturnStatement;
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


public class TreeBuilder implements ParserVisitor {
	class RootNode extends JexlNode {
		public RootNode(int id) {
			super(id);
		}

		public RootNode(Parser p, int id) {
			super(p, id);
		}
	}

	private TreeNode rootNode = null;

	private TreeNode currentNode = null;

	private boolean currentlyInCheckChildren = false;

	public TreeBuilder(String query) throws ParseException {
		Parser p = new Parser(new StringReader(";"));
		ASTJexlScript script = p.parse(new StringReader(query), null);
		rootNode = new TreeNode();
		rootNode.setType(TreeBuilder.RootNode.class);
		currentNode = rootNode;
		QueryParser.EvaluationContext ctx = new QueryParser.EvaluationContext();
		script.childrenAccept(this, ctx);
	}

	public TreeBuilder(ASTJexlScript script) {
		rootNode = new TreeNode();
		rootNode.setType(TreeBuilder.RootNode.class);
		currentNode = rootNode;
		QueryParser.EvaluationContext ctx = new QueryParser.EvaluationContext();
		script.childrenAccept(this, ctx);
	}

	public TreeNode getRootNode() {
		return this.rootNode;
	}

	public Object visit(SimpleNode node, Object data) {
		return null;
	}

	public Object visit(ASTJexlScript node, Object data) {
		return null;
	}

	public Object visit(ASTWhileStatement node, Object data) {
		return null;
	}

	public Object visit(ASTAssignment node, Object data) {
		return null;
	}

	private boolean nodeCheck(JexlNode node, Class<?> failClass) {
		if ((node.getClass().equals(failClass)) || (node.getClass().equals(ASTNotNode.class)))
			return false;
		else {
			for (int i = 0; i < (node.jjtGetNumChildren()); i++) {
				if (!(nodeCheck(node.jjtGetChild(i), failClass)))
					return false;

			}
		}
		return true;
	}

	private Multimap<String, QueryParser.QueryTerm> checkChildren(JexlNode parent, QueryParser.EvaluationContext ctx) {
		this.currentlyInCheckChildren = true;
		Multimap<String, QueryParser.QueryTerm> rolledUpTerms = null;
		boolean result = false;
		if (parent.getClass().equals(ASTOrNode.class)) {
			for (int i = 0; i < (parent.jjtGetNumChildren()); i++) {
				result = nodeCheck(parent.jjtGetChild(i), ASTAndNode.class);
				if (!result)
					break;

			}
		}else {
			for (int i = 0; i < (parent.jjtGetNumChildren()); i++) {
				result = nodeCheck(parent.jjtGetChild(i), ASTOrNode.class);
				if (!result)
					break;

			}
		}
		if (result) {
			TreeNode rollupFakeNode = new TreeNode();
			TreeNode previous = this.currentNode;
			this.currentNode = rollupFakeNode;
			parent.childrenAccept(this, ctx);
			rolledUpTerms = this.currentNode.getTerms();
			this.currentNode = previous;
		}
		this.currentlyInCheckChildren = false;
		return rolledUpTerms;
	}

	public Object visit(ASTBitwiseOrNode node, Object data) {
		return null;
	}

	public Object visit(ASTLTNode node, Object data) {
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
		this.currentNode.getTerms().put(fieldName.toString(), term);
		return null;
	}

	public Object visit(ASTGTNode node, Object data) {
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
		this.currentNode.getTerms().put(fieldName.toString(), term);
		return null;
	}

	public Object visit(ASTBitwiseComplNode node, Object data) {
		return null;
	}

	public Object visit(ASTFalseNode node, Object data) {
		return new QueryParser.LiteralResult(node.image);
	}

	public Object visit(ASTIntegerLiteral node, Object data) {
		return new QueryParser.LiteralResult(node.image);
	}

	public Object visit(ASTFloatLiteral node, Object data) {
		return new QueryParser.LiteralResult(node.image);
	}

	public Object visit(ASTSizeMethod node, Object data) {
		return null;
	}

	public Object visit(ASTConstructorNode node, Object data) {
		return null;
	}

	private void decodeResults(Object left, Object right, StringBuilder fieldName, QueryParser.ObjectHolder holder) {
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

	public Object visit(ASTBitwiseXorNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTNRNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTIfStatement para0, Object para1) {
		return null;
	}

	public Object visit(ASTMapLiteral para0, Object para1) {
		return null;
	}

	public Object visit(ASTBitwiseAndNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTFunctionNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTERNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTSizeFunction para0, Object para1) {
		return null;
	}

	public Object visit(ASTEQNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTNotNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTMethodNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTAdditiveNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTStringLiteral para0, Object para1) {
		return null;
	}

	public Object visit(ASTForeachStatement para0, Object para1) {
		return null;
	}

	public Object visit(ASTAdditiveOperator para0, Object para1) {
		return null;
	}

	public Object visit(ASTIdentifier para0, Object para1) {
		return null;
	}

	public Object visit(ASTLENode para0, Object para1) {
		return null;
	}

	public Object visit(ASTUnaryMinusNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTBlock para0, Object para1) {
		return null;
	}

	public Object visit(ASTDivNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTModNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTAmbiguous para0, Object para1) {
		return null;
	}

	public Object visit(ASTOrNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTGENode para0, Object para1) {
		return null;
	}

	public Object visit(ASTNullLiteral para0, Object para1) {
		return null;
	}

	public Object visit(ASTTernaryNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTArrayLiteral para0, Object para1) {
		return null;
	}

	public Object visit(ASTNENode para0, Object para1) {
		return null;
	}

	public Object visit(ASTMulNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTEmptyFunction para0, Object para1) {
		return null;
	}

	public Object visit(ASTTrueNode para0, Object para1) {
		return null;
	}

	public Object visit(ASTMapEntry para0, Object para1) {
		return null;
	}

	public Object visit(ASTNumberLiteral para0, Object para1) {
		return null;
	}

	public Object visit(ASTVar para0, Object para1) {
		return null;
	}

	public Object visit(ASTArrayAccess para0, Object para1) {
		return null;
	}

	public Object visit(ASTReturnStatement para0, Object para1) {
		return null;
	}

	public Object visit(ASTReference para0, Object para1) {
		return null;
	}

	public Object visit(ASTAndNode para0, Object para1) {
		return null;
	}
}


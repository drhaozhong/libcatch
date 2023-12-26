package org.apache.accumulo.examples.wikisearch.parser;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;


public class JexlOperatorConstants implements ParserTreeConstants {
	private static Map<Class<? extends JexlNode>, String> operatorMap = new ConcurrentHashMap<Class<? extends JexlNode>, String>();

	private static Map<String, Class<? extends JexlNode>> classMap = new ConcurrentHashMap<String, Class<? extends JexlNode>>();

	private static Map<Integer, String> jjtOperatorMap = new ConcurrentHashMap<Integer, String>();

	private static Map<String, Integer> jjtTypeMap = new ConcurrentHashMap<String, Integer>();

	static {
		JexlOperatorConstants.operatorMap.put(ASTEQNode.class, "==");
		JexlOperatorConstants.operatorMap.put(ASTNENode.class, "!=");
		JexlOperatorConstants.operatorMap.put(ASTLTNode.class, "<");
		JexlOperatorConstants.operatorMap.put(ASTLENode.class, "<=");
		JexlOperatorConstants.operatorMap.put(ASTGTNode.class, ">");
		JexlOperatorConstants.operatorMap.put(ASTGENode.class, ">=");
		JexlOperatorConstants.operatorMap.put(ASTERNode.class, "=~");
		JexlOperatorConstants.operatorMap.put(ASTNRNode.class, "!~");
		JexlOperatorConstants.operatorMap.put(ASTFunctionNode.class, "f");
		JexlOperatorConstants.operatorMap.put(ASTAndNode.class, "and");
		JexlOperatorConstants.operatorMap.put(ASTOrNode.class, "or");
		JexlOperatorConstants.classMap.put("==", ASTEQNode.class);
		JexlOperatorConstants.classMap.put("!=", ASTNENode.class);
		JexlOperatorConstants.classMap.put("<", ASTLTNode.class);
		JexlOperatorConstants.classMap.put("<=", ASTLENode.class);
		JexlOperatorConstants.classMap.put(">", ASTGTNode.class);
		JexlOperatorConstants.classMap.put(">=", ASTGENode.class);
		JexlOperatorConstants.classMap.put("=~", ASTERNode.class);
		JexlOperatorConstants.classMap.put("!~", ASTNRNode.class);
		JexlOperatorConstants.classMap.put("f", ASTFunctionNode.class);
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTEQNODE, "==");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTNENODE, "!=");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTLTNODE, "<");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTLENODE, "<=");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTGTNODE, ">");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTGENODE, ">=");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTERNODE, "=~");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTNRNODE, "!~");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTFUNCTIONNODE, "f");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTANDNODE, "and");
		JexlOperatorConstants.jjtOperatorMap.put(ParserTreeConstants.JJTORNODE, "or");
		JexlOperatorConstants.jjtTypeMap.put("==", ParserTreeConstants.JJTEQNODE);
		JexlOperatorConstants.jjtTypeMap.put("!=", ParserTreeConstants.JJTNENODE);
		JexlOperatorConstants.jjtTypeMap.put("<", ParserTreeConstants.JJTLTNODE);
		JexlOperatorConstants.jjtTypeMap.put("<=", ParserTreeConstants.JJTLENODE);
		JexlOperatorConstants.jjtTypeMap.put(">", ParserTreeConstants.JJTGTNODE);
		JexlOperatorConstants.jjtTypeMap.put(">=", ParserTreeConstants.JJTGENODE);
		JexlOperatorConstants.jjtTypeMap.put("=~", ParserTreeConstants.JJTERNODE);
		JexlOperatorConstants.jjtTypeMap.put("!~", ParserTreeConstants.JJTNRNODE);
		JexlOperatorConstants.jjtTypeMap.put("f", ParserTreeConstants.JJTFUNCTIONNODE);
	}

	public static String getOperator(Class<? extends JexlNode> nodeType) {
		return JexlOperatorConstants.operatorMap.get(nodeType);
	}

	public static String getOperator(Integer jjtNode) {
		return JexlOperatorConstants.jjtOperatorMap.get(jjtNode);
	}

	public static Class<? extends JexlNode> getClass(String operator) {
		return JexlOperatorConstants.classMap.get(operator);
	}

	public static int getJJTNodeType(String operator) {
		return JexlOperatorConstants.jjtTypeMap.get(operator);
	}
}


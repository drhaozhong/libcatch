package org.apache.accumulo.examples.wikisearch.parser;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import javax.swing.tree.DefaultMutableTreeNode;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class FieldIndexQueryReWriter {
	protected static final Logger log = Logger.getLogger(FieldIndexQueryReWriter.class);

	public static final String INDEXED_TERMS_LIST = "INDEXED_TERMS_LIST";

	public static Set<Integer> rangeNodeSet;

	static {
		FieldIndexQueryReWriter.rangeNodeSet = new HashSet<Integer>();
		FieldIndexQueryReWriter.rangeNodeSet.add(ParserTreeConstants.JJTLENODE);
		FieldIndexQueryReWriter.rangeNodeSet.add(ParserTreeConstants.JJTLTNODE);
		FieldIndexQueryReWriter.rangeNodeSet.add(ParserTreeConstants.JJTGENODE);
		FieldIndexQueryReWriter.rangeNodeSet.add(ParserTreeConstants.JJTGTNODE);
		FieldIndexQueryReWriter.rangeNodeSet = Collections.unmodifiableSet(FieldIndexQueryReWriter.rangeNodeSet);
	}

	public static void setLogLevel(Level lev) {
		FieldIndexQueryReWriter.log.setLevel(lev);
	}

	public String removeNonIndexedTermsAndInvalidRanges(String query, Map<String, String> options) throws Exception, ParseException {
		Multimap<String, String> indexedTerms = parseIndexedTerms(options);
		FieldIndexQueryReWriter.RewriterTreeNode node = parseJexlQuery(query);
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("Tree: " + (node.getContents())));
		}
		node = removeNonIndexedTerms(node, indexedTerms);
		node = removeTreeConflicts(node, indexedTerms);
		node = collapseBranches(node);
		node = removeNegationViolations(node);
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("Tree -NonIndexed: " + (node.getContents())));
		}
		return rebuildQueryFromTree(node);
	}

	public String applyNormalizedTerms(String query, Map<String, String> options) throws Exception, ParseException {
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("applyNormalizedTerms, query: " + query));
		}
		Multimap<String, String> normalizedTerms = parseIndexedTerms(options);
		FieldIndexQueryReWriter.RewriterTreeNode node = parseJexlQuery(query);
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("applyNormalizedTerms, Tree: " + (node.getContents())));
		}
		node = orNormalizedTerms(node, normalizedTerms);
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("applyNormalizedTerms,Normalized: " + (node.getContents())));
		}
		return rebuildQueryFromTree(node);
	}

	public String applyCaseSensitivity(String query, boolean fNameUpper, boolean fValueUpper) throws ParseException {
		FieldIndexQueryReWriter.RewriterTreeNode node = parseJexlQuery(query);
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("Tree: " + (node.getContents())));
		}
		node = applyCaseSensitivity(node, fNameUpper, fValueUpper);
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("Case: " + (node.getContents())));
		}
		return rebuildQueryFromTree(node);
	}

	private String rebuildQueryFromTree(FieldIndexQueryReWriter.RewriterTreeNode node) {
		if (node.isLeaf()) {
			String fName = node.getFieldName();
			String fValue = node.getFieldValue();
			String operator = node.getOperator();
			if (node.isNegated()) {
				if ((node.getType()) == (JexlOperatorConstants.JJTEQNODE)) {
					operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTNENODE);
				}else
					if ((node.getType()) == (JexlOperatorConstants.JJTERNODE)) {
						operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTNRNODE);
					}else
						if ((node.getType()) == (JexlOperatorConstants.JJTLTNODE)) {
							operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTGENODE);
						}else
							if ((node.getType()) == (JexlOperatorConstants.JJTLENODE)) {
								operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTGTNODE);
							}else
								if ((node.getType()) == (JexlOperatorConstants.JJTGTNODE)) {
									operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTLENODE);
								}else
									if ((node.getType()) == (JexlOperatorConstants.JJTGENODE)) {
										operator = JexlOperatorConstants.getOperator(JexlOperatorConstants.JJTLTNODE);
									}





			}
			return (((fName + operator) + "'") + fValue) + "'";
		}else {
			List<String> parts = new ArrayList<String>();
			Enumeration<?> children = node.children();
			while (children.hasMoreElements()) {
				FieldIndexQueryReWriter.RewriterTreeNode child = ((FieldIndexQueryReWriter.RewriterTreeNode) (children.nextElement()));
				parts.add(rebuildQueryFromTree(child));
			} 
			if ((node.getType()) == (ParserTreeConstants.JJTJEXLSCRIPT)) {
				return StringUtils.join(parts, "");
			}
			String op = (" " + (JexlOperatorConstants.getOperator(node.getType()))) + " ";
			if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				FieldIndexQueryReWriter.log.debug(("Operator: " + op));
			}
			String query = StringUtils.join(parts, op);
			query = ("(" + query) + ")";
			return query;
		}
	}

	private FieldIndexQueryReWriter.RewriterTreeNode parseJexlQuery(String query) throws ParseException {
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("parseJexlQuery, query: " + query));
		}
		QueryParser parser = new QueryParser();
		parser.execute(query);
		TreeNode tree = parser.getIteratorTree();
		FieldIndexQueryReWriter.RewriterTreeNode root = transformTreeNode(tree);
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("parseJexlQuery, transformedTree: " + (root.getContents())));
		}
		root = refactorTree(root);
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug(("parseJexlQuery, refactorTree: " + (root.getContents())));
		}
		return root;
	}

	private FieldIndexQueryReWriter.RewriterTreeNode transformTreeNode(TreeNode node) throws ParseException {
		if ((node.getType().equals(ASTEQNode.class)) || (node.getType().equals(ASTNENode.class))) {
			if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				FieldIndexQueryReWriter.log.debug("transformTreeNode, Equals Node");
			}
			Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
			for (String fName : terms.keySet()) {
				Collection<QueryParser.QueryTerm> values = terms.get(fName);
				for (QueryParser.QueryTerm t : values) {
					if ((null == t) || (null == (t.getValue()))) {
						continue;
					}
					String fValue = t.getValue().toString();
					fValue = fValue.replaceAll("'", "");
					boolean negated = t.getOperator().equals("!=");
					FieldIndexQueryReWriter.RewriterTreeNode child = new FieldIndexQueryReWriter.RewriterTreeNode(ParserTreeConstants.JJTEQNODE, fName, fValue, negated);
					return child;
				}
			}
		}
		if ((node.getType().equals(ASTERNode.class)) || (node.getType().equals(ASTNRNode.class))) {
			if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				FieldIndexQueryReWriter.log.debug("transformTreeNode, Regex Node");
			}
			Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
			for (String fName : terms.keySet()) {
				Collection<QueryParser.QueryTerm> values = terms.get(fName);
				for (QueryParser.QueryTerm t : values) {
					if ((null == t) || (null == (t.getValue()))) {
						continue;
					}
					String fValue = t.getValue().toString();
					fValue = fValue.replaceAll("'", "");
					boolean negated = node.getType().equals(ASTNRNode.class);
					FieldIndexQueryReWriter.RewriterTreeNode child = new FieldIndexQueryReWriter.RewriterTreeNode(ParserTreeConstants.JJTERNODE, fName, fValue, negated);
					return child;
				}
			}
		}
		if ((((node.getType().equals(ASTLTNode.class)) || (node.getType().equals(ASTLENode.class))) || (node.getType().equals(ASTGTNode.class))) || (node.getType().equals(ASTGENode.class))) {
			if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				FieldIndexQueryReWriter.log.debug("transformTreeNode, LT/LE/GT/GE node");
			}
			Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
			for (String fName : terms.keySet()) {
				Collection<QueryParser.QueryTerm> values = terms.get(fName);
				for (QueryParser.QueryTerm t : values) {
					if ((null == t) || (null == (t.getValue()))) {
						continue;
					}
					String fValue = t.getValue().toString();
					fValue = fValue.replaceAll("'", "").toLowerCase();
					boolean negated = false;
					int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
					FieldIndexQueryReWriter.RewriterTreeNode child = new FieldIndexQueryReWriter.RewriterTreeNode(mytype, fName, fValue, negated);
					return child;
				}
			}
		}
		FieldIndexQueryReWriter.RewriterTreeNode returnNode = null;
		if ((node.getType().equals(ASTAndNode.class)) || (node.getType().equals(ASTOrNode.class))) {
			int parentType = (node.getType().equals(ASTAndNode.class)) ? ParserTreeConstants.JJTANDNODE : ParserTreeConstants.JJTORNODE;
			if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				FieldIndexQueryReWriter.log.debug(("transformTreeNode, AND/OR node: " + parentType));
			}
			if ((node.isLeaf()) || (!(node.getTerms().isEmpty()))) {
				returnNode = new FieldIndexQueryReWriter.RewriterTreeNode(parentType);
				Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
				for (String fName : terms.keySet()) {
					Collection<QueryParser.QueryTerm> values = terms.get(fName);
					for (QueryParser.QueryTerm t : values) {
						if ((null == t) || (null == (t.getValue()))) {
							continue;
						}
						String fValue = t.getValue().toString();
						fValue = fValue.replaceAll("'", "");
						boolean negated = t.getOperator().equals("!=");
						int childType = JexlOperatorConstants.getJJTNodeType(t.getOperator());
						FieldIndexQueryReWriter.RewriterTreeNode child = new FieldIndexQueryReWriter.RewriterTreeNode(childType, fName, fValue, negated);
						if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
							FieldIndexQueryReWriter.log.debug(("adding child node: " + (child.getContents())));
						}
						returnNode.add(child);
					}
				}
			}else {
				returnNode = new FieldIndexQueryReWriter.RewriterTreeNode(parentType);
			}
		}else
			if (node.getType().equals(ASTNotNode.class)) {
				if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
					FieldIndexQueryReWriter.log.debug("transformTreeNode, NOT node");
				}
				if (node.isLeaf()) {
					Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
					for (String fName : terms.keySet()) {
						Collection<QueryParser.QueryTerm> values = terms.get(fName);
						for (QueryParser.QueryTerm t : values) {
							if ((null == t) || (null == (t.getValue()))) {
								continue;
							}
							String fValue = t.getValue().toString();
							fValue = fValue.replaceAll("'", "").toLowerCase();
							boolean negated = !(t.getOperator().equals("!="));
							int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
							return new FieldIndexQueryReWriter.RewriterTreeNode(mytype, fName, fValue, negated);
						}
					}
				}else {
					returnNode = new FieldIndexQueryReWriter.RewriterTreeNode(ParserTreeConstants.JJTNOTNODE);
				}
			}else
				if ((node.getType().equals(ASTJexlScript.class)) || (node.getType().getSimpleName().equals("RootNode"))) {
					if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
						FieldIndexQueryReWriter.log.debug("transformTreeNode, ROOT/JexlScript node");
					}
					if (node.isLeaf()) {
						returnNode = new FieldIndexQueryReWriter.RewriterTreeNode(ParserTreeConstants.JJTJEXLSCRIPT);
						Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
						for (String fName : terms.keySet()) {
							Collection<QueryParser.QueryTerm> values = terms.get(fName);
							for (QueryParser.QueryTerm t : values) {
								if ((null == t) || (null == (t.getValue()))) {
									continue;
								}
								String fValue = t.getValue().toString();
								fValue = fValue.replaceAll("'", "");
								boolean negated = t.getOperator().equals("!=");
								int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
								FieldIndexQueryReWriter.RewriterTreeNode child = new FieldIndexQueryReWriter.RewriterTreeNode(mytype, fName, fValue, negated);
								returnNode.add(child);
								return returnNode;
							}
						}
					}else {
						returnNode = new FieldIndexQueryReWriter.RewriterTreeNode(ParserTreeConstants.JJTJEXLSCRIPT);
					}
				}else {
					FieldIndexQueryReWriter.log.error(((("transformTreeNode,  Currently Unsupported Node type: " + (node.getClass().getName())) + " \t") + (node.getType())));
				}


		for (TreeNode child : node.getChildren()) {
			returnNode.add(transformTreeNode(child));
		}
		return returnNode;
	}

	private FieldIndexQueryReWriter.RewriterTreeNode removeNonIndexedTerms(FieldIndexQueryReWriter.RewriterTreeNode root, Multimap<String, String> indexedTerms) throws Exception {
		if (indexedTerms.isEmpty()) {
			throw new Exception("removeNonIndexedTerms, indexed Terms empty");
		}
		List<FieldIndexQueryReWriter.RewriterTreeNode> nodes = new ArrayList<FieldIndexQueryReWriter.RewriterTreeNode>();
		Enumeration<?> bfe = root.breadthFirstEnumeration();
		while (bfe.hasMoreElements()) {
			FieldIndexQueryReWriter.RewriterTreeNode node = ((FieldIndexQueryReWriter.RewriterTreeNode) (bfe.nextElement()));
			nodes.add(node);
		} 
		for (int i = (nodes.size()) - 1; i >= 0; i--) {
			FieldIndexQueryReWriter.RewriterTreeNode node = nodes.get(i);
			if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				FieldIndexQueryReWriter.log.debug(((("removeNonIndexedTerms, analyzing node: " + (node.toString())) + "  ") + (node.printNode())));
			}
			if (((node.getType()) == (ParserTreeConstants.JJTANDNODE)) || ((node.getType()) == (ParserTreeConstants.JJTORNODE))) {
				if ((node.getChildCount()) == 0) {
					node.removeFromParent();
				}else
					if ((node.getChildCount()) == 1) {
						FieldIndexQueryReWriter.RewriterTreeNode p = ((FieldIndexQueryReWriter.RewriterTreeNode) (node.getParent()));
						FieldIndexQueryReWriter.RewriterTreeNode c = ((FieldIndexQueryReWriter.RewriterTreeNode) (node.getFirstChild()));
						node.removeFromParent();
						p.add(c);
					}

			}else
				if ((node.getType()) == (ParserTreeConstants.JJTJEXLSCRIPT)) {
					if ((node.getChildCount()) == 0) {
						throw new Exception();
					}
				}else
					if (FieldIndexQueryReWriter.rangeNodeSet.contains(node.getType())) {
						continue;
					}else {
						if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
							FieldIndexQueryReWriter.log.debug(((("removeNonIndexedTerms, Testing: " + (node.getFieldName())) + ":") + (node.getFieldValue())));
						}
						if (!(indexedTerms.containsKey((((node.getFieldName().toString()) + ":") + (node.getFieldValue().toString()))))) {
							if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
								FieldIndexQueryReWriter.log.debug(((((node.getFieldName()) + ":") + (node.getFieldValue())) + " is NOT indexed"));
							}
							node.removeFromParent();
						}else {
							if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
								FieldIndexQueryReWriter.log.debug(((((node.getFieldName()) + ":") + (node.getFieldValue())) + " is indexed"));
							}
						}
					}


		}
		return root;
	}

	private FieldIndexQueryReWriter.RewriterTreeNode orNormalizedTerms(FieldIndexQueryReWriter.RewriterTreeNode myroot, Multimap<String, String> indexedTerms) throws Exception {
		if (indexedTerms.isEmpty()) {
			throw new Exception("indexed Terms empty");
		}
		try {
			List<FieldIndexQueryReWriter.RewriterTreeNode> nodes = new ArrayList<FieldIndexQueryReWriter.RewriterTreeNode>();
			Enumeration<?> bfe = myroot.breadthFirstEnumeration();
			while (bfe.hasMoreElements()) {
				FieldIndexQueryReWriter.RewriterTreeNode node = ((FieldIndexQueryReWriter.RewriterTreeNode) (bfe.nextElement()));
				nodes.add(node);
			} 
			for (int i = (nodes.size()) - 1; i >= 0; i--) {
				FieldIndexQueryReWriter.RewriterTreeNode node = nodes.get(i);
				if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
					FieldIndexQueryReWriter.log.debug(((("orNormalizedTerms, analyzing node: " + (node.toString())) + "  ") + (node.printNode())));
				}
				if (((node.getType()) == (ParserTreeConstants.JJTANDNODE)) || ((node.getType()) == (ParserTreeConstants.JJTORNODE))) {
					continue;
				}else
					if ((node.getType()) == (ParserTreeConstants.JJTJEXLSCRIPT)) {
						if ((node.getChildCount()) == 0) {
							if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
								FieldIndexQueryReWriter.log.debug("orNormalizedTerms: Head node has no children!");
							}
							throw new Exception();
						}
					}else {
						if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
							FieldIndexQueryReWriter.log.debug(("Testing data location: " + (node.getFieldName())));
						}
						String fName = node.getFieldName().toString();
						String fValue = node.getFieldValue().toString();
						if (indexedTerms.containsKey(((fName + ":") + fValue))) {
							if ((indexedTerms.get(((fName + ":") + fValue)).size()) > 1) {
								node.setType(ParserTreeConstants.JJTORNODE);
								boolean neg = node.isNegated();
								node.setNegated(false);
								node.setFieldName(null);
								node.setFieldValue(null);
								Collection<String> values = indexedTerms.get(((fName + ":") + fValue));
								for (String value : values) {
									FieldIndexQueryReWriter.RewriterTreeNode n = new FieldIndexQueryReWriter.RewriterTreeNode(ParserTreeConstants.JJTEQNODE, fName, value, neg);
									node.add(n);
								}
							}else
								if ((indexedTerms.get(((fName + ":") + fValue)).size()) == 1) {
									Collection<String> values = indexedTerms.get(((fName + ":") + fValue));
									for (String val : values) {
										node.setFieldValue(val);
									}
								}

						}else {
						}
					}

			}
		} catch (Exception e) {
			FieldIndexQueryReWriter.log.debug(("Caught exception in orNormalizedTerms(): " + e));
			throw new Exception("exception in: orNormalizedTerms");
		}
		return myroot;
	}

	private FieldIndexQueryReWriter.RewriterTreeNode removeTreeConflicts(FieldIndexQueryReWriter.RewriterTreeNode root, Multimap<String, String> indexedTerms) {
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug("removeTreeConflicts");
		}
		List<FieldIndexQueryReWriter.RewriterTreeNode> nodeList = new ArrayList<FieldIndexQueryReWriter.RewriterTreeNode>();
		Enumeration<?> nodes = root.breadthFirstEnumeration();
		while (nodes.hasMoreElements()) {
			FieldIndexQueryReWriter.RewriterTreeNode child = ((FieldIndexQueryReWriter.RewriterTreeNode) (nodes.nextElement()));
			nodeList.add(child);
		} 
		for (int i = (nodeList.size()) - 1; i >= 0; i--) {
			FieldIndexQueryReWriter.RewriterTreeNode node = nodeList.get(i);
			if (node.isRemoval()) {
				node.removeFromParent();
				continue;
			}
			FieldIndexQueryReWriter.RewriterTreeNode parent = ((FieldIndexQueryReWriter.RewriterTreeNode) (node.getParent()));
			if (((node.getType()) == (ParserTreeConstants.JJTANDNODE)) && (((node.getLevel()) == 1) || (((parent.getType()) == (ParserTreeConstants.JJTORNODE)) && ((parent.getLevel()) == 1)))) {
				if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
					FieldIndexQueryReWriter.log.debug("AND at level 1 or with OR parent at level 1");
				}
				Map<Text, RangeCalculator.RangeBounds> rangeMap = getBoundedRangeMap(node);
				List<FieldIndexQueryReWriter.RewriterTreeNode> childList = new ArrayList<FieldIndexQueryReWriter.RewriterTreeNode>();
				Enumeration<?> children = node.children();
				while (children.hasMoreElements()) {
					FieldIndexQueryReWriter.RewriterTreeNode child = ((FieldIndexQueryReWriter.RewriterTreeNode) (children.nextElement()));
					childList.add(child);
				} 
				for (int j = (childList.size()) - 1; j >= 0; j--) {
					FieldIndexQueryReWriter.RewriterTreeNode child = childList.get(j);
					if (FieldIndexQueryReWriter.rangeNodeSet.contains(child.getType())) {
						if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
							FieldIndexQueryReWriter.log.debug(("child type: " + (JexlOperatorConstants.getOperator(child.getType()))));
						}
						if (rangeMap == null) {
							child.removeFromParent();
						}else {
							if (!(rangeMap.containsKey(new Text(child.getFieldName())))) {
								child.removeFromParent();
							}else {
								boolean singleSib = false;
								if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
									FieldIndexQueryReWriter.log.debug("checking for singleSib.");
								}
								Enumeration<?> sibs = child.getParent().children();
								while (sibs.hasMoreElements()) {
									FieldIndexQueryReWriter.RewriterTreeNode sib = ((FieldIndexQueryReWriter.RewriterTreeNode) (sibs.nextElement()));
									if (!(FieldIndexQueryReWriter.rangeNodeSet.contains(sib.getType()))) {
										singleSib = true;
										break;
									}
								} 
								if (singleSib) {
									child.removeFromParent();
								}else {
									if (indexedTerms.containsKey((((child.getFieldName()) + ":") + (child.getFieldValue())))) {
										if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
											FieldIndexQueryReWriter.log.debug(("removeTreeConflicts, node: " + (node.getContents())));
										}
										node.removeAllChildren();
										node.setType(ParserTreeConstants.JJTORNODE);
										Collection<String> values = indexedTerms.get((((child.getFieldName()) + ":") + (child.getFieldValue())));
										for (String value : values) {
											FieldIndexQueryReWriter.RewriterTreeNode n = new FieldIndexQueryReWriter.RewriterTreeNode(ParserTreeConstants.JJTEQNODE, child.getFieldName(), value, child.isNegated());
											node.add(n);
										}
										if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
											FieldIndexQueryReWriter.log.debug(("removeTreeConflicts, node: " + (node.getContents())));
										}
										break;
									}else {
										child.removeFromParent();
									}
								}
							}
						}
					}
				}
			}else {
				if (node.isLeaf()) {
					continue;
				}
				List<FieldIndexQueryReWriter.RewriterTreeNode> childList = new ArrayList<FieldIndexQueryReWriter.RewriterTreeNode>();
				Enumeration<?> children = node.children();
				while (children.hasMoreElements()) {
					FieldIndexQueryReWriter.RewriterTreeNode child = ((FieldIndexQueryReWriter.RewriterTreeNode) (children.nextElement()));
					childList.add(child);
				} 
				for (int j = (childList.size()) - 1; j >= 0; j--) {
					FieldIndexQueryReWriter.RewriterTreeNode child = childList.get(j);
					if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
						FieldIndexQueryReWriter.log.debug(("removeTreeConflicts, looking at node: " + node));
					}
					if (FieldIndexQueryReWriter.rangeNodeSet.contains(child.getType())) {
						FieldIndexQueryReWriter.RewriterTreeNode grandParent = ((FieldIndexQueryReWriter.RewriterTreeNode) (child.getParent().getParent()));
						if (((grandParent.getType()) == (ParserTreeConstants.JJTORNODE)) && ((grandParent.getLevel()) != 1)) {
							grandParent.setRemoval(true);
						}
						child.removeFromParent();
					}
				}
			}
		}
		return root;
	}

	private FieldIndexQueryReWriter.RewriterTreeNode removeNegationViolations(FieldIndexQueryReWriter.RewriterTreeNode node) throws Exception {
		FieldIndexQueryReWriter.RewriterTreeNode one = ((FieldIndexQueryReWriter.RewriterTreeNode) (node.getFirstChild()));
		ArrayList<FieldIndexQueryReWriter.RewriterTreeNode> childrenList = new ArrayList<FieldIndexQueryReWriter.RewriterTreeNode>();
		Enumeration<?> children = one.children();
		while (children.hasMoreElements()) {
			FieldIndexQueryReWriter.RewriterTreeNode child = ((FieldIndexQueryReWriter.RewriterTreeNode) (children.nextElement()));
			childrenList.add(child);
		} 
		if ((one.getType()) == (JexlOperatorConstants.JJTORNODE)) {
			for (FieldIndexQueryReWriter.RewriterTreeNode child : childrenList) {
				if (child.isNegated()) {
					child.removeFromParent();
				}
			}
			if ((one.getChildCount()) == 0) {
				throw new Exception("FieldIndexQueryReWriter: Top level query node cannot be processed.");
			}
		}else
			if ((one.getType()) == (JexlOperatorConstants.JJTANDNODE)) {
				boolean ok = false;
				for (FieldIndexQueryReWriter.RewriterTreeNode child : childrenList) {
					if (!(child.isNegated())) {
						ok = true;
						break;
					}
				}
				if (!ok) {
					throw new Exception("FieldIndexQueryReWriter: Top level query node cannot be processed.");
				}
			}

		return node;
	}

	private FieldIndexQueryReWriter.RewriterTreeNode collapseBranches(FieldIndexQueryReWriter.RewriterTreeNode myroot) throws Exception {
		List<FieldIndexQueryReWriter.RewriterTreeNode> nodes = new ArrayList<FieldIndexQueryReWriter.RewriterTreeNode>();
		Enumeration<?> bfe = myroot.breadthFirstEnumeration();
		while (bfe.hasMoreElements()) {
			FieldIndexQueryReWriter.RewriterTreeNode node = ((FieldIndexQueryReWriter.RewriterTreeNode) (bfe.nextElement()));
			nodes.add(node);
		} 
		for (int i = (nodes.size()) - 1; i >= 0; i--) {
			FieldIndexQueryReWriter.RewriterTreeNode node = nodes.get(i);
			if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				FieldIndexQueryReWriter.log.debug(((("collapseBranches, inspecting node: " + (node.toString())) + "  ") + (node.printNode())));
			}
			if (((node.getType()) == (ParserTreeConstants.JJTANDNODE)) || ((node.getType()) == (ParserTreeConstants.JJTORNODE))) {
				if ((node.getChildCount()) == 0) {
					node.removeFromParent();
				}else
					if ((node.getChildCount()) == 1) {
						FieldIndexQueryReWriter.RewriterTreeNode p = ((FieldIndexQueryReWriter.RewriterTreeNode) (node.getParent()));
						FieldIndexQueryReWriter.RewriterTreeNode c = ((FieldIndexQueryReWriter.RewriterTreeNode) (node.getFirstChild()));
						node.removeFromParent();
						p.add(c);
					}

			}else
				if ((node.getType()) == (ParserTreeConstants.JJTJEXLSCRIPT)) {
					if ((node.getChildCount()) == 0) {
						throw new Exception();
					}
				}

		}
		return myroot;
	}

	public Multimap<String, String> parseIndexedTerms(Map<String, String> options) {
		if ((options.get(FieldIndexQueryReWriter.INDEXED_TERMS_LIST)) != null) {
			Multimap<String, String> mmap = HashMultimap.create();
			String[] items = options.get(FieldIndexQueryReWriter.INDEXED_TERMS_LIST).split(";");
			for (String item : items) {
				item = item.trim();
				if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				}
				String[] parts = item.split(":");
				if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
					FieldIndexQueryReWriter.log.debug(("adding: " + (parts[0])));
				}
				for (int i = 2; i < (parts.length); i++) {
					mmap.put((((parts[0]) + ":") + (parts[1])), parts[i]);
				}
			}
			if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				FieldIndexQueryReWriter.log.debug(("multimap: " + mmap));
			}
			return mmap;
		}
		if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
			FieldIndexQueryReWriter.log.debug("parseIndexedTerms: returning null");
		}
		return null;
	}

	public FieldIndexQueryReWriter.RewriterTreeNode refactorTree(FieldIndexQueryReWriter.RewriterTreeNode root) {
		Enumeration<?> dfe = root.breadthFirstEnumeration();
		while (dfe.hasMoreElements()) {
			FieldIndexQueryReWriter.RewriterTreeNode n = ((FieldIndexQueryReWriter.RewriterTreeNode) (dfe.nextElement()));
			if ((n.getType()) == (ParserTreeConstants.JJTNOTNODE)) {
				FieldIndexQueryReWriter.RewriterTreeNode child = ((FieldIndexQueryReWriter.RewriterTreeNode) (n.getChildAt(0)));
				child.setNegated(true);
				FieldIndexQueryReWriter.RewriterTreeNode parent = ((FieldIndexQueryReWriter.RewriterTreeNode) (n.getParent()));
				parent.remove(n);
				parent.add(child);
			}
		} 
		Enumeration<?> bfe = root.breadthFirstEnumeration();
		FieldIndexQueryReWriter.RewriterTreeNode child;
		while (bfe.hasMoreElements()) {
			child = ((FieldIndexQueryReWriter.RewriterTreeNode) (bfe.nextElement()));
			if (child.isNegated()) {
				if ((child.getChildCount()) > 0) {
					demorganSubTree(child);
				}
			}
		} 
		return root;
	}

	private void demorganSubTree(FieldIndexQueryReWriter.RewriterTreeNode root) {
		root.setNegated(false);
		if ((root.getType()) == (ParserTreeConstants.JJTANDNODE)) {
			root.setType(ParserTreeConstants.JJTORNODE);
		}else
			if ((root.getType()) == (ParserTreeConstants.JJTORNODE)) {
				root.setType(ParserTreeConstants.JJTANDNODE);
			}else
				if (((root.getType()) == (ParserTreeConstants.JJTEQNODE)) || ((root.getType()) == (ParserTreeConstants.JJTERNODE))) {
				}else {
					FieldIndexQueryReWriter.log.error("refactorSubTree, node type not supported");
				}


		Enumeration<?> children = root.children();
		FieldIndexQueryReWriter.RewriterTreeNode child = null;
		while (children.hasMoreElements()) {
			child = ((FieldIndexQueryReWriter.RewriterTreeNode) (children.nextElement()));
			if (child.isNegated()) {
				child.setNegated(false);
			}else {
				child.setNegated(true);
			}
		} 
	}

	private FieldIndexQueryReWriter.RewriterTreeNode applyCaseSensitivity(FieldIndexQueryReWriter.RewriterTreeNode root, boolean fnUpper, boolean fvUpper) {
		Enumeration<?> bfe = root.breadthFirstEnumeration();
		while (bfe.hasMoreElements()) {
			FieldIndexQueryReWriter.RewriterTreeNode node = ((FieldIndexQueryReWriter.RewriterTreeNode) (bfe.nextElement()));
			if (node.isLeaf()) {
				String fName = (fnUpper) ? node.getFieldName().toUpperCase() : node.getFieldName().toLowerCase();
				node.setFieldName(fName);
				String fValue = (fvUpper) ? node.getFieldValue().toUpperCase() : node.getFieldValue().toLowerCase();
				node.setFieldValue(fValue);
			}
		} 
		return root;
	}

	private Map<Text, RangeCalculator.RangeBounds> getBoundedRangeMap(FieldIndexQueryReWriter.RewriterTreeNode node) {
		if (((node.getType()) == (ParserTreeConstants.JJTANDNODE)) || ((node.getType()) == (ParserTreeConstants.JJTORNODE))) {
			Enumeration<?> children = node.children();
			Map<Text, RangeCalculator.RangeBounds> rangeMap = new HashMap<Text, RangeCalculator.RangeBounds>();
			while (children.hasMoreElements()) {
				FieldIndexQueryReWriter.RewriterTreeNode child = ((FieldIndexQueryReWriter.RewriterTreeNode) (children.nextElement()));
				if (((child.getType()) == (ParserTreeConstants.JJTLENODE)) || ((child.getType()) == (ParserTreeConstants.JJTLTNODE))) {
					Text fName = new Text(child.getFieldName());
					if (rangeMap.containsKey(fName)) {
						RangeCalculator.RangeBounds rb = rangeMap.get(fName);
						if ((rb.getLower()) != null) {
							FieldIndexQueryReWriter.log.error("testBoundedRangeExistence, two lower bounds exist for bounded range.");
						}
						rb.setLower(new Text(child.getFieldValue()));
					}else {
						RangeCalculator.RangeBounds rb = new RangeCalculator.RangeBounds();
						rb.setLower(new Text(child.getFieldValue()));
						rangeMap.put(new Text(child.getFieldName()), rb);
					}
				}else
					if (((child.getType()) == (ParserTreeConstants.JJTGENODE)) || ((child.getType()) == (ParserTreeConstants.JJTGTNODE))) {
						Text fName = new Text(child.getFieldName());
						if (rangeMap.containsKey(fName)) {
							RangeCalculator.RangeBounds rb = rangeMap.get(fName);
							if ((rb.getUpper()) != null) {
								FieldIndexQueryReWriter.log.error("testBoundedRangeExistence, two Upper bounds exist for bounded range.");
							}
							rb.setUpper(new Text(child.getFieldValue()));
						}else {
							RangeCalculator.RangeBounds rb = new RangeCalculator.RangeBounds();
							rb.setUpper(new Text(child.getFieldValue()));
							rangeMap.put(new Text(child.getFieldName()), rb);
						}
					}

			} 
			for (Map.Entry<Text, RangeCalculator.RangeBounds> entry : rangeMap.entrySet()) {
				RangeCalculator.RangeBounds rb = entry.getValue();
				if (((rb.getLower()) == null) || ((rb.getUpper()) == null)) {
					if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
						FieldIndexQueryReWriter.log.debug("testBoundedRangeExistence: Unbounded Range detected, removing entry from rangeMap");
					}
					rangeMap.remove(entry.getKey());
				}
			}
			if (!(rangeMap.isEmpty())) {
				return rangeMap;
			}
		}
		return null;
	}

	public class RewriterTreeNode extends DefaultMutableTreeNode {
		private static final long serialVersionUID = 1L;

		private boolean negated = false;

		private String fieldName;

		private String fieldValue;

		private String operator;

		private int type;

		private boolean removal = false;

		public RewriterTreeNode(int type) {
			super();
			this.type = type;
		}

		public RewriterTreeNode(int type, String fName, String fValue) {
			super();
			init(type, fName, fValue);
		}

		public RewriterTreeNode(int type, String fName, String fValue, boolean negate) {
			super();
			init(type, fName, fValue, negate);
		}

		private void init(int type, String fName, String fValue) {
			init(type, fName, fValue, false);
		}

		private void init(int type, String fName, String fValue, boolean negate) {
			this.type = type;
			this.fieldName = fName;
			this.fieldValue = fValue;
			this.negated = negate;
			this.operator = JexlOperatorConstants.getOperator(type);
			if (FieldIndexQueryReWriter.log.isDebugEnabled()) {
				FieldIndexQueryReWriter.log.debug(((((("FN: " + (this.fieldName)) + "  FV: ") + (this.fieldValue)) + " Op: ") + (this.operator)));
			}
		}

		public String getFieldName() {
			return fieldName;
		}

		public void setFieldName(String fieldName) {
			this.fieldName = fieldName;
		}

		public String getFieldValue() {
			return fieldValue;
		}

		public void setFieldValue(String fieldValue) {
			this.fieldValue = fieldValue;
		}

		public boolean isNegated() {
			return negated;
		}

		public void setNegated(boolean negated) {
			this.negated = negated;
		}

		public String getOperator() {
			return operator;
		}

		public void setOperator(String operator) {
			this.operator = operator;
		}

		public int getType() {
			return type;
		}

		public void setType(int type) {
			this.type = type;
		}

		public boolean isRemoval() {
			return removal;
		}

		public void setRemoval(boolean removal) {
			this.removal = removal;
		}

		public String getContents() {
			StringBuilder s = new StringBuilder("[");
			s.append(toString());
			if ((children) != null) {
				Enumeration<?> e = this.children();
				while (e.hasMoreElements()) {
					FieldIndexQueryReWriter.RewriterTreeNode n = ((FieldIndexQueryReWriter.RewriterTreeNode) (e.nextElement()));
					s.append(",");
					s.append(n.getContents());
				} 
			}
			s.append("]");
			return s.toString();
		}

		public String printNode() {
			StringBuilder s = new StringBuilder("[");
			s.append("Full Location & Term = ");
			if ((this.fieldName) != null) {
				s.append(this.fieldName.toString());
			}else {
				s.append("BlankDataLocation");
			}
			s.append("  ");
			if ((this.fieldValue) != null) {
				s.append(this.fieldValue.toString());
			}else {
				s.append("BlankTerm");
			}
			s.append("]");
			return s.toString();
		}

		@Override
		public String toString() {
			switch (type) {
				case ParserTreeConstants.JJTEQNODE :
					return ((((fieldName) + ":") + (fieldValue)) + ":negated=") + (isNegated());
				case ParserTreeConstants.JJTNENODE :
					return ((((fieldName) + ":") + (fieldValue)) + ":negated=") + (isNegated());
				case ParserTreeConstants.JJTERNODE :
					return ((((fieldName) + ":") + (fieldValue)) + ":negated=") + (isNegated());
				case ParserTreeConstants.JJTNRNODE :
					return ((((fieldName) + ":") + (fieldValue)) + ":negated=") + (isNegated());
				case ParserTreeConstants.JJTLENODE :
					return ((((fieldName) + ":") + (fieldValue)) + ":negated=") + (isNegated());
				case ParserTreeConstants.JJTLTNODE :
					return ((((fieldName) + ":") + (fieldValue)) + ":negated=") + (isNegated());
				case ParserTreeConstants.JJTGENODE :
					return ((((fieldName) + ":") + (fieldValue)) + ":negated=") + (isNegated());
				case ParserTreeConstants.JJTGTNODE :
					return ((((fieldName) + ":") + (fieldValue)) + ":negated=") + (isNegated());
				case ParserTreeConstants.JJTJEXLSCRIPT :
					return "HEAD";
				case ParserTreeConstants.JJTANDNODE :
					return "AND";
				case ParserTreeConstants.JJTNOTNODE :
					return "NOT";
				case ParserTreeConstants.JJTORNODE :
					return "OR";
				default :
					System.out.println("Problem in NuwaveTreeNode.toString()");
					return null;
			}
		}
	}
}


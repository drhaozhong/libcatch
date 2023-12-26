package org.apache.accumulo.examples.wikisearch.iterator;


import com.google.common.collect.Multimap;
import java.io.IOException;
import java.io.PrintStream;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import javax.swing.tree.DefaultMutableTreeNode;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.examples.wikisearch.iterator.BooleanLogicTreeNode;
import org.apache.accumulo.examples.wikisearch.parser.JexlOperatorConstants;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser;
import org.apache.accumulo.examples.wikisearch.parser.QueryParser.QueryTerm;
import org.apache.accumulo.examples.wikisearch.parser.RangeCalculator;
import org.apache.accumulo.examples.wikisearch.parser.TreeNode;
import org.apache.accumulo.examples.wikisearch.util.FieldIndexKeyParser;
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
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class BooleanLogicIterator implements OptionDescriber , SortedKeyValueIterator<Key, Value> {
	private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

	protected static final Logger log = Logger.getLogger(BooleanLogicIterator.class);

	public static final String QUERY_OPTION = "expr";

	public static final String TERM_CARDINALITIES = "TERM_CARDINALITIES";

	public static final String FIELD_INDEX_QUERY = "FIELD_INDEX_QUERY";

	public static final String FIELD_NAME_PREFIX = "fi\u0000";

	private static IteratorEnvironment env = new DefaultIteratorEnvironment();

	protected Text nullText = new Text();

	private Key topKey = null;

	private Value topValue = null;

	private SortedKeyValueIterator<Key, Value> sourceIterator;

	private BooleanLogicTreeNode root;

	private PriorityQueue<BooleanLogicTreeNode> positives;

	private ArrayList<BooleanLogicTreeNode> negatives = new ArrayList<BooleanLogicTreeNode>();

	private ArrayList<BooleanLogicTreeNode> rangerators;

	private String updatedQuery;

	private Map<String, Long> termCardinalities = new HashMap<String, Long>();

	private Range overallRange = null;

	private FieldIndexKeyParser keyParser;

	public BooleanLogicIterator() {
		keyParser = new FieldIndexKeyParser();
		rangerators = new ArrayList<BooleanLogicTreeNode>();
	}

	public BooleanLogicIterator(BooleanLogicIterator other, IteratorEnvironment env) {
		if ((other.sourceIterator) != null) {
			this.sourceIterator = other.sourceIterator.deepCopy(env);
		}
		keyParser = new FieldIndexKeyParser();
		rangerators = new ArrayList<BooleanLogicTreeNode>();
		BooleanLogicIterator.log.debug("Congratulations, you've reached the BooleanLogicIterator");
	}

	public static void setLogLevel(Level lev) {
		BooleanLogicIterator.log.setLevel(lev);
	}

	public void setDebug(Level lev) {
		BooleanLogicIterator.log.setLevel(lev);
	}

	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new BooleanLogicIterator(this, env);
	}

	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		validateOptions(options);
		try {
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("Congratulations, you've reached the BooleanLogicIterator.init method");
			}
			sourceIterator = source.deepCopy(env);
			String[] terms = null;
			if (null != (options.get(BooleanLogicIterator.TERM_CARDINALITIES))) {
				terms = options.get(BooleanLogicIterator.TERM_CARDINALITIES).split(",");
				for (String term : terms) {
					int idx = term.indexOf(":");
					if ((-1) != idx) {
						termCardinalities.put(term.substring(0, idx), Long.parseLong(term.substring((idx + 1))));
					}
				}
			}
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("QueryParser");
			}
			QueryParser qp = new QueryParser();
			qp.execute(this.updatedQuery);
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("transformTreeNode");
			}
			TreeNode tree = qp.getIteratorTree();
			this.root = transformTreeNode(tree);
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("refactorTree");
			}
			this.root = refactorTree(this.root);
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("collapseBranches");
			}
			BooleanLogicIterator.collapseBranches(root);
			createIteratorTree(this.root);
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug(("Query tree after iterator creation:\n\t" + (this.root.getContents())));
			}
			splitLeaves(this.root);
		} catch (ParseException ex) {
			BooleanLogicIterator.log.error(("ParseException in init: " + ex));
			throw new IllegalArgumentException("Failed to parse query", ex);
		} catch (Exception ex) {
			throw new IllegalArgumentException("probably had no indexed terms", ex);
		}
	}

	private void createIteratorTree(BooleanLogicTreeNode root) throws IOException {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug("BoolLogic createIteratorTree()");
		}
		Enumeration<?> dfe = root.depthFirstEnumeration();
		while (dfe.hasMoreElements()) {
			BooleanLogicTreeNode node = ((BooleanLogicTreeNode) (dfe.nextElement()));
			if ((!(node.isLeaf())) && ((node.getType()) != (ParserTreeConstants.JJTJEXLSCRIPT))) {
				if (BooleanLogicIterator.canRollUp(node)) {
					node.setRollUp(true);
					if ((node.getType()) == (ParserTreeConstants.JJTANDNODE)) {
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug("creating IntersectingIterator");
						}
						node.setUserObject(createIntersectingIterator(node));
					}else
						if ((node.getType()) == (ParserTreeConstants.JJTORNODE)) {
							node.setUserObject(createOrIterator(node));
						}else {
							BooleanLogicIterator.log.debug(("createIteratorTree, encounterd a node type I do not know about: " + (node.getType())));
							BooleanLogicIterator.log.debug(("createIteratorTree, node contents:  " + (node.getContents())));
						}

					node.removeAllChildren();
				}
			}
		} 
		dfe = root.depthFirstEnumeration();
		while (dfe.hasMoreElements()) {
			BooleanLogicTreeNode node = ((BooleanLogicTreeNode) (dfe.nextElement()));
			if (((node.isLeaf()) && ((node.getType()) != (ParserTreeConstants.JJTANDNODE))) && ((node.getType()) != (ParserTreeConstants.JJTORNODE))) {
				node.setUserObject(createFieldIndexIterator(node));
			}
		} 
	}

	private AndIterator createIntersectingIterator(BooleanLogicTreeNode node) throws IOException {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug("createIntersectingIterator(node)");
			BooleanLogicIterator.log.debug(((((("fName: " + (node.getFieldName())) + " , fValue: ") + (node.getFieldValue())) + " , operator: ") + (node.getFieldOperator())));
		}
		Text[] columnFamilies = new Text[node.getChildCount()];
		Text[] termValues = new Text[node.getChildCount()];
		boolean[] negationMask = new boolean[node.getChildCount()];
		Enumeration<?> children = node.children();
		int i = 0;
		while (children.hasMoreElements()) {
			BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (children.nextElement()));
			columnFamilies[i] = child.getFieldName();
			termValues[i] = child.getFieldValue();
			negationMask[i] = child.isNegated();
			i++;
		} 
		AndIterator ii = new AndIterator();
		Map<String, String> options = new HashMap<String, String>();
		options.put(AndIterator.columnFamiliesOptionName, AndIterator.encodeColumns(columnFamilies));
		options.put(AndIterator.termValuesOptionName, AndIterator.encodeTermValues(termValues));
		options.put(AndIterator.notFlagsOptionName, AndIterator.encodeBooleans(negationMask));
		ii.init(sourceIterator.deepCopy(BooleanLogicIterator.env), options, BooleanLogicIterator.env);
		return ii;
	}

	private OrIterator createOrIterator(BooleanLogicTreeNode node) throws IOException {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug("createOrIterator(node)");
			BooleanLogicIterator.log.debug(((((("fName: " + (node.getFieldName())) + " , fValue: ") + (node.getFieldValue())) + " , operator: ") + (node.getFieldOperator())));
		}
		Enumeration<?> children = node.children();
		ArrayList<Text> fams = new ArrayList<Text>();
		ArrayList<Text> quals = new ArrayList<Text>();
		while (children.hasMoreElements()) {
			BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (children.nextElement()));
			fams.add(child.getFieldName());
			quals.add(child.getFieldValue());
		} 
		OrIterator iter = new OrIterator();
		SortedKeyValueIterator<Key, Value> source = sourceIterator.deepCopy(BooleanLogicIterator.env);
		for (int i = 0; i < (fams.size()); i++) {
			iter.addTerm(source, fams.get(i), quals.get(i), BooleanLogicIterator.env);
		}
		return iter;
	}

	private FieldIndexIterator createFieldIndexIterator(BooleanLogicTreeNode node) throws IOException {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug("BoolLogic.createFieldIndexIterator()");
			BooleanLogicIterator.log.debug(((((("fName: " + (node.getFieldName())) + " , fValue: ") + (node.getFieldValue())) + " , operator: ") + (node.getFieldOperator())));
		}
		Text rowId = null;
		sourceIterator.seek(new Range(), BooleanLogicIterator.EMPTY_COL_FAMS, false);
		if (sourceIterator.hasTop()) {
			rowId = sourceIterator.getTopKey().getRow();
		}
		FieldIndexIterator iter = new FieldIndexIterator(node.getType(), rowId, node.getFieldName(), node.getFieldValue(), node.isNegated(), node.getFieldOperator());
		Map<String, String> options = new HashMap<String, String>();
		iter.init(sourceIterator.deepCopy(BooleanLogicIterator.env), options, BooleanLogicIterator.env);
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			FieldIndexIterator.setLogLevel(Level.DEBUG);
		}else {
			FieldIndexIterator.setLogLevel(Level.OFF);
		}
		return iter;
	}

	private boolean testTreeState() {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug("BoolLogic testTreeState() begin");
		}
		Enumeration<?> dfe = this.root.depthFirstEnumeration();
		while (dfe.hasMoreElements()) {
			BooleanLogicTreeNode node = ((BooleanLogicTreeNode) (dfe.nextElement()));
			if (!(node.isLeaf())) {
				int type = node.getType();
				if (type == (ParserTreeConstants.JJTANDNODE)) {
					handleAND(node);
				}else
					if (type == (ParserTreeConstants.JJTORNODE)) {
						handleOR(node);
					}else
						if (type == (ParserTreeConstants.JJTJEXLSCRIPT)) {
							handleHEAD(node);
						}else
							if (type == (ParserTreeConstants.JJTNOTNODE)) {
							}



			}else {
				if ((node.getType()) == (ParserTreeConstants.JJTORNODE)) {
					node.setValid(node.hasTop());
					node.reSet();
					node.addToSet(node.getTopKey());
				}else
					if ((((((((node.getType()) == (ParserTreeConstants.JJTANDNODE)) || ((node.getType()) == (ParserTreeConstants.JJTEQNODE))) || ((node.getType()) == (ParserTreeConstants.JJTERNODE))) || ((node.getType()) == (ParserTreeConstants.JJTLENODE))) || ((node.getType()) == (ParserTreeConstants.JJTLTNODE))) || ((node.getType()) == (ParserTreeConstants.JJTGENODE))) || ((node.getType()) == (ParserTreeConstants.JJTGTNODE))) {
						node.setValid(node.hasTop());
					}

			}
		} 
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug(((("BoolLogic.testTreeState end, treeState:: " + (this.root.getContents())) + "  , valid: ") + (root.isValid())));
		}
		return this.root.isValid();
	}

	private void handleHEAD(BooleanLogicTreeNode node) {
		Enumeration<?> children = node.children();
		while (children.hasMoreElements()) {
			BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (children.nextElement()));
			if ((child.getType()) == (ParserTreeConstants.JJTANDNODE)) {
				node.setValid(child.isValid());
				node.setTopKey(child.getTopKey());
			}else
				if ((child.getType()) == (ParserTreeConstants.JJTORNODE)) {
					node.setValid(child.isValid());
					node.setTopKey(child.getTopKey());
				}else
					if (((((((child.getType()) == (ParserTreeConstants.JJTEQNODE)) || ((child.getType()) == (ParserTreeConstants.JJTERNODE))) || ((child.getType()) == (ParserTreeConstants.JJTGTNODE))) || ((child.getType()) == (ParserTreeConstants.JJTGENODE))) || ((child.getType()) == (ParserTreeConstants.JJTLTNODE))) || ((child.getType()) == (ParserTreeConstants.JJTLENODE))) {
						node.setValid(true);
						node.setTopKey(child.getTopKey());
						if ((child.getTopKey()) == null) {
							node.setValid(false);
						}
					}


		} 
		if ((node.isValid()) && (!(node.hasTop()))) {
			node.setValid(false);
		}
	}

	private void handleAND(BooleanLogicTreeNode me) {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug(("handleAND::" + (me.getContents())));
		}
		Enumeration<?> children = me.children();
		me.setValid(true);
		HashSet<Key> goodSet = new HashSet<Key>();
		HashSet<Key> badSet = new HashSet<Key>();
		while (children.hasMoreElements()) {
			BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (children.nextElement()));
			if (((((((((child.getType()) == (ParserTreeConstants.JJTEQNODE)) || ((child.getType()) == (ParserTreeConstants.JJTANDNODE))) || ((child.getType()) == (ParserTreeConstants.JJTERNODE))) || ((child.getType()) == (ParserTreeConstants.JJTNENODE))) || ((child.getType()) == (ParserTreeConstants.JJTGENODE))) || ((child.getType()) == (ParserTreeConstants.JJTLENODE))) || ((child.getType()) == (ParserTreeConstants.JJTGTNODE))) || ((child.getType()) == (ParserTreeConstants.JJTLTNODE))) {
				if (child.isNegated()) {
					if (child.hasTop()) {
						badSet.add(child.getTopKey());
						if (goodSet.contains(child.getTopKey())) {
							me.setValid(false);
							return;
						}
						if (child.isValid()) {
							me.setValid(false);
							return;
						}
					}
				}else {
					if (child.hasTop()) {
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug(("handleAND, child node: " + (child.getContents())));
						}
						if (badSet.contains(child.getTopKey())) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug("handleAND, child is in bad set, setting parent false");
							}
							me.setValid(false);
							return;
						}
						if (goodSet.isEmpty()) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug(("handleAND, goodSet is empty, adding child: " + (child.getContents())));
							}
							goodSet.add(child.getTopKey());
						}else {
							if (!(goodSet.contains(child.getTopKey()))) {
								if (BooleanLogicIterator.log.isDebugEnabled()) {
									BooleanLogicIterator.log.debug(("handleAND, goodSet is not empty, and does NOT contain child, setting false.  child: " + (child.getContents())));
								}
								me.setValid(false);
								return;
							}else {
								goodSet = new HashSet<Key>();
								goodSet.add(child.getTopKey());
								if (BooleanLogicIterator.log.isDebugEnabled()) {
									BooleanLogicIterator.log.debug(("handleAND, child in goodset, trim to this value: " + (child.getContents())));
								}
							}
						}
					}else {
						if ((child.getChildCount()) > 0) {
							Enumeration<?> subchildren = child.children();
							boolean allFalse = true;
							while (subchildren.hasMoreElements()) {
								BooleanLogicTreeNode subchild = ((BooleanLogicTreeNode) (subchildren.nextElement()));
								if (!(subchild.isNegated())) {
									allFalse = false;
									break;
								}else
									if ((subchild.isNegated()) && (subchild.hasTop())) {
										allFalse = false;
										break;
									}

							} 
							if (!allFalse) {
								me.setValid(false);
								return;
							}
						}else {
							me.setValid(false);
							return;
						}
					}
				}
			}else
				if ((child.getType()) == (ParserTreeConstants.JJTORNODE)) {
					Iterator<?> iter = child.getSetIterator();
					boolean goodSetEmpty = goodSet.isEmpty();
					boolean matchedOne = false;
					boolean pureNegations = true;
					if (!(child.isValid())) {
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug(("handleAND, child is an OR and it is not valid, setting false, ALL NEGATED?: " + (child.isChildrenAllNegated())));
						}
						me.setValid(false);
						return;
					}else
						if ((child.isValid()) && (!(child.hasTop()))) {
						}else
							if ((child.isValid()) && (child.hasTop())) {
								if (BooleanLogicIterator.log.isDebugEnabled()) {
									BooleanLogicIterator.log.debug("handleAND, child OR, valid and has top, means not pureNegations");
								}
								pureNegations = false;
								while (iter.hasNext()) {
									Key i = ((Key) (iter.next()));
									if (child.isNegated()) {
										badSet.add(i);
										if (goodSet.contains(i)) {
											if (BooleanLogicIterator.log.isDebugEnabled()) {
												BooleanLogicIterator.log.debug(("handleAND, child OR, goodSet contains bad value: " + i));
											}
											me.setValid(false);
											return;
										}
									}else {
										if (goodSetEmpty && (!(badSet.contains(i)))) {
											goodSet.add(i);
											matchedOne = true;
										}else {
											if (goodSet.contains(i)) {
												matchedOne = true;
											}
										}
									}
								} 
							}


					if (child.isNegated()) {
					}else {
						if ((goodSet.isEmpty()) && (!pureNegations)) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug("handleAND, child OR, empty goodset && !pureNegations, set false");
							}
							me.setValid(false);
							return;
						}else
							if ((!(goodSet.isEmpty())) && (!pureNegations)) {
								if (!matchedOne) {
									if (BooleanLogicIterator.log.isDebugEnabled()) {
										BooleanLogicIterator.log.debug("handleAND, child OR, goodSet had values but I didn't match any, false");
									}
									me.setValid(false);
									return;
								}
								goodSet = child.getIntersection(goodSet);
							}

					}
				}

		} 
		if (goodSet.isEmpty()) {
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("handleAND-> goodSet is empty, pure negations?");
			}
		}else {
			me.setTopKey(Collections.min(goodSet));
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug(("End of handleAND, this node's topKey: " + (me.getTopKey())));
			}
		}
	}

	private void handleOR(BooleanLogicTreeNode me) {
		Enumeration<?> children = me.children();
		me.setValid(false);
		me.reSet();
		me.setTopKey(null);
		boolean allNegated = true;
		while (children.hasMoreElements()) {
			BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (children.nextElement()));
			if ((((((((((child.getType()) == (ParserTreeConstants.JJTEQNODE)) || ((child.getType()) == (ParserTreeConstants.JJTNENODE))) || ((child.getType()) == (ParserTreeConstants.JJTANDNODE))) || ((child.getType()) == (ParserTreeConstants.JJTERNODE))) || ((child.getType()) == (ParserTreeConstants.JJTNRNODE))) || ((child.getType()) == (ParserTreeConstants.JJTLENODE))) || ((child.getType()) == (ParserTreeConstants.JJTLTNODE))) || ((child.getType()) == (ParserTreeConstants.JJTGENODE))) || ((child.getType()) == (ParserTreeConstants.JJTGTNODE))) {
				if (child.hasTop()) {
					if (child.isNegated()) {
					}else {
						allNegated = false;
						if (child.isValid()) {
							me.addToSet(child.getTopKey());
						}
					}
				}else
					if (!(child.isNegated())) {
						allNegated = false;
						me.setValid(child.isValid());
					}

			}else
				if ((child.getType()) == (ParserTreeConstants.JJTORNODE)) {
					if (child.hasTop()) {
						if (!(child.isNegated())) {
							allNegated = false;
							Iterator<?> iter = child.getSetIterator();
							while (iter.hasNext()) {
								Key i = ((Key) (iter.next()));
								if (i != null) {
									me.addToSet(i);
								}
							} 
						}
					}else {
						if (child.isValid()) {
							me.setValid(true);
						}
					}
				}

		} 
		if (allNegated) {
			children = me.children();
			while (children.hasMoreElements()) {
				BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (children.nextElement()));
				if (!(child.hasTop())) {
					me.setValid(true);
					me.setTopKey(null);
					return;
				}
			} 
			me.setValid(false);
		}else {
			Key k = me.getMinUniqueID();
			if (k == null) {
				me.setValid(false);
			}else {
				me.setValid(true);
				me.setTopKey(k);
			}
		}
	}

	public BooleanLogicTreeNode transformTreeNode(TreeNode node) throws ParseException {
		if ((node.getType().equals(ASTEQNode.class)) || (node.getType().equals(ASTNENode.class))) {
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("Equals Node");
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
					if (!(fName.startsWith(BooleanLogicIterator.FIELD_NAME_PREFIX))) {
						fName = (BooleanLogicIterator.FIELD_NAME_PREFIX) + fName;
					}
					BooleanLogicTreeNode child = new BooleanLogicTreeNode(ParserTreeConstants.JJTEQNODE, fName, fValue, negated);
					return child;
				}
			}
		}
		if ((node.getType().equals(ASTERNode.class)) || (node.getType().equals(ASTNRNode.class))) {
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("Regex Node");
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
					if (!(fName.startsWith(BooleanLogicIterator.FIELD_NAME_PREFIX))) {
						fName = (BooleanLogicIterator.FIELD_NAME_PREFIX) + fName;
					}
					BooleanLogicTreeNode child = new BooleanLogicTreeNode(ParserTreeConstants.JJTERNODE, fName, fValue, negated);
					return child;
				}
			}
		}
		if ((((node.getType().equals(ASTLTNode.class)) || (node.getType().equals(ASTLENode.class))) || (node.getType().equals(ASTGTNode.class))) || (node.getType().equals(ASTGENode.class))) {
			Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
			for (String fName : terms.keySet()) {
				Collection<QueryParser.QueryTerm> values = terms.get(fName);
				if (!(fName.startsWith(BooleanLogicIterator.FIELD_NAME_PREFIX))) {
					fName = (BooleanLogicIterator.FIELD_NAME_PREFIX) + fName;
				}
				for (QueryParser.QueryTerm t : values) {
					if ((null == t) || (null == (t.getValue()))) {
						continue;
					}
					String fValue = t.getValue().toString();
					fValue = fValue.replaceAll("'", "").toLowerCase();
					boolean negated = false;
					int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
					BooleanLogicTreeNode child = new BooleanLogicTreeNode(mytype, fName, fValue, negated);
					if (BooleanLogicIterator.log.isDebugEnabled()) {
						BooleanLogicIterator.log.debug(("adding child node: " + (child.getContents())));
					}
					return child;
				}
			}
		}
		BooleanLogicTreeNode returnNode = null;
		if ((node.getType().equals(ASTAndNode.class)) || (node.getType().equals(ASTOrNode.class))) {
			int parentType = (node.getType().equals(ASTAndNode.class)) ? ParserTreeConstants.JJTANDNODE : ParserTreeConstants.JJTORNODE;
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug(("AND/OR node: " + parentType));
			}
			if ((node.isLeaf()) || (!(node.getTerms().isEmpty()))) {
				returnNode = new BooleanLogicTreeNode(parentType);
				Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
				for (String fName : terms.keySet()) {
					Collection<QueryParser.QueryTerm> values = terms.get(fName);
					if (!(fName.startsWith(BooleanLogicIterator.FIELD_NAME_PREFIX))) {
						fName = (BooleanLogicIterator.FIELD_NAME_PREFIX) + fName;
					}
					for (QueryParser.QueryTerm t : values) {
						if ((null == t) || (null == (t.getValue()))) {
							continue;
						}
						String fValue = t.getValue().toString();
						fValue = fValue.replaceAll("'", "");
						boolean negated = t.getOperator().equals("!=");
						int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
						BooleanLogicTreeNode child = new BooleanLogicTreeNode(mytype, fName, fValue, negated);
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug(("adding child node: " + (child.getContents())));
						}
						returnNode.add(child);
					}
				}
			}else {
				returnNode = new BooleanLogicTreeNode(parentType);
			}
		}else
			if (node.getType().equals(ASTNotNode.class)) {
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					BooleanLogicIterator.log.debug("NOT node");
				}
				if (node.isLeaf()) {
					Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
					for (String fName : terms.keySet()) {
						Collection<QueryParser.QueryTerm> values = terms.get(fName);
						if (!(fName.startsWith(BooleanLogicIterator.FIELD_NAME_PREFIX))) {
							fName = (BooleanLogicIterator.FIELD_NAME_PREFIX) + fName;
						}
						for (QueryParser.QueryTerm t : values) {
							if ((null == t) || (null == (t.getValue()))) {
								continue;
							}
							String fValue = t.getValue().toString();
							fValue = fValue.replaceAll("'", "").toLowerCase();
							boolean negated = !(t.getOperator().equals("!="));
							int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
							if (!(fName.startsWith(BooleanLogicIterator.FIELD_NAME_PREFIX))) {
								fName = (BooleanLogicIterator.FIELD_NAME_PREFIX) + fName;
							}
							return new BooleanLogicTreeNode(mytype, fName, fValue, negated);
						}
					}
				}else {
					returnNode = new BooleanLogicTreeNode(ParserTreeConstants.JJTNOTNODE);
				}
			}else
				if ((node.getType().equals(ASTJexlScript.class)) || (node.getType().getSimpleName().equals("RootNode"))) {
					if (BooleanLogicIterator.log.isDebugEnabled()) {
						BooleanLogicIterator.log.debug("ROOT/JexlScript node");
					}
					if (node.isLeaf()) {
						returnNode = new BooleanLogicTreeNode(ParserTreeConstants.JJTJEXLSCRIPT);
						Multimap<String, QueryParser.QueryTerm> terms = node.getTerms();
						for (String fName : terms.keySet()) {
							Collection<QueryParser.QueryTerm> values = terms.get(fName);
							if (!(fName.startsWith(BooleanLogicIterator.FIELD_NAME_PREFIX))) {
								fName = (BooleanLogicIterator.FIELD_NAME_PREFIX) + fName;
							}
							for (QueryParser.QueryTerm t : values) {
								if ((null == t) || (null == (t.getValue()))) {
									continue;
								}
								String fValue = t.getValue().toString();
								fValue = fValue.replaceAll("'", "").toLowerCase();
								boolean negated = t.getOperator().equals("!=");
								int mytype = JexlOperatorConstants.getJJTNodeType(t.getOperator());
								BooleanLogicTreeNode child = new BooleanLogicTreeNode(mytype, fName, fValue, negated);
								returnNode.add(child);
								return returnNode;
							}
						}
					}else {
						returnNode = new BooleanLogicTreeNode(ParserTreeConstants.JJTJEXLSCRIPT);
					}
				}else {
					BooleanLogicIterator.log.error(((("Currently Unsupported Node type: " + (node.getClass().getName())) + " \t") + (node.getType())));
				}


		for (TreeNode child : node.getChildren()) {
			returnNode.add(transformTreeNode(child));
		}
		return returnNode;
	}

	public static void collapseBranches(BooleanLogicTreeNode myroot) throws Exception {
		List<BooleanLogicTreeNode> nodes = new ArrayList<BooleanLogicTreeNode>();
		Enumeration<?> bfe = myroot.breadthFirstEnumeration();
		while (bfe.hasMoreElements()) {
			BooleanLogicTreeNode node = ((BooleanLogicTreeNode) (bfe.nextElement()));
			nodes.add(node);
		} 
		for (int i = (nodes.size()) - 1; i >= 0; i--) {
			BooleanLogicTreeNode node = nodes.get(i);
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug(((("collapseBranches, inspecting node: " + (node.toString())) + "  ") + (node.printNode())));
			}
			if (((node.getType()) == (ParserTreeConstants.JJTANDNODE)) || ((node.getType()) == (ParserTreeConstants.JJTORNODE))) {
				if (((node.getChildCount()) == 0) && (!(node.isRangeNode()))) {
					node.removeFromParent();
				}else
					if ((node.getChildCount()) == 1) {
						BooleanLogicTreeNode p = ((BooleanLogicTreeNode) (node.getParent()));
						BooleanLogicTreeNode c = ((BooleanLogicTreeNode) (node.getFirstChild()));
						node.removeFromParent();
						p.add(c);
					}

			}else
				if ((node.getType()) == (ParserTreeConstants.JJTJEXLSCRIPT)) {
					if ((node.getChildCount()) == 0) {
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug("collapseBranches, headNode has no children");
						}
						throw new Exception("Head node has no children.");
					}
				}

		}
	}

	public BooleanLogicTreeNode refactorTree(BooleanLogicTreeNode myroot) {
		List<BooleanLogicTreeNode> nodes = new ArrayList<BooleanLogicTreeNode>();
		Enumeration<?> bfe = myroot.breadthFirstEnumeration();
		while (bfe.hasMoreElements()) {
			BooleanLogicTreeNode node = ((BooleanLogicTreeNode) (bfe.nextElement()));
			nodes.add(node);
		} 
		for (int i = (nodes.size()) - 1; i >= 0; i--) {
			BooleanLogicTreeNode node = nodes.get(i);
			if (((node.getType()) == (ParserTreeConstants.JJTANDNODE)) || ((node.getType()) == (ParserTreeConstants.JJTORNODE))) {
				Map<Text, RangeCalculator.RangeBounds> ranges = new HashMap<Text, RangeCalculator.RangeBounds>();
				Enumeration<?> children = node.children();
				boolean allNegated = true;
				while (children.hasMoreElements()) {
					BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (children.nextElement()));
					if (!(child.isNegated())) {
						allNegated = false;
					}
					if ((node.getType()) == (ParserTreeConstants.JJTANDNODE)) {
						if ((child.getType()) == (JexlOperatorConstants.JJTGTNODE)) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug(("refactor: GT " + (child.getContents())));
							}
							if (ranges.containsKey(child.getFieldName())) {
								RangeCalculator.RangeBounds rb = ranges.get(child.getFieldName());
								rb.setLower(child.getFieldValue());
							}else {
								RangeCalculator.RangeBounds rb = new RangeCalculator.RangeBounds();
								rb.setLower(child.getFieldValue());
								ranges.put(child.getFieldName(), rb);
							}
						}else
							if ((child.getType()) == (JexlOperatorConstants.JJTGENODE)) {
								if (BooleanLogicIterator.log.isDebugEnabled()) {
									BooleanLogicIterator.log.debug(("refactor: GE " + (child.getContents())));
								}
								if (ranges.containsKey(child.getFieldName())) {
									RangeCalculator.RangeBounds rb = ranges.get(child.getFieldName());
									rb.setLower(child.getFieldValue());
								}else {
									RangeCalculator.RangeBounds rb = new RangeCalculator.RangeBounds();
									rb.setLower(child.getFieldValue());
									ranges.put(child.getFieldName(), rb);
								}
							}else
								if ((child.getType()) == (JexlOperatorConstants.JJTLTNODE)) {
									if (BooleanLogicIterator.log.isDebugEnabled()) {
										BooleanLogicIterator.log.debug(("refactor: LT " + (child.getContents())));
									}
									if (ranges.containsKey(child.getFieldName())) {
										RangeCalculator.RangeBounds rb = ranges.get(child.getFieldName());
										rb.setUpper(child.getFieldValue());
									}else {
										RangeCalculator.RangeBounds rb = new RangeCalculator.RangeBounds();
										rb.setUpper(child.getFieldValue());
										ranges.put(child.getFieldName(), rb);
									}
								}else
									if ((child.getType()) == (JexlOperatorConstants.JJTLENODE)) {
										if (BooleanLogicIterator.log.isDebugEnabled()) {
											BooleanLogicIterator.log.debug(("refactor: LE " + (child.getContents())));
										}
										if (ranges.containsKey(child.getFieldName())) {
											RangeCalculator.RangeBounds rb = ranges.get(child.getFieldName());
											rb.setUpper(child.getFieldValue());
										}else {
											RangeCalculator.RangeBounds rb = new RangeCalculator.RangeBounds();
											rb.setUpper(child.getFieldValue());
											ranges.put(child.getFieldName(), rb);
										}
									}



					}
				} 
				if (allNegated) {
					node.setChildrenAllNegated(true);
				}
				if ((node.getType()) == (ParserTreeConstants.JJTANDNODE)) {
					if (!(ranges.isEmpty())) {
						if (((node.getChildCount()) <= 2) && ((ranges.size()) == 1)) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug("AND range 2 children or less");
							}
							node.setType(ParserTreeConstants.JJTORNODE);
							node.removeAllChildren();
							for (Map.Entry<Text, RangeCalculator.RangeBounds> entry : ranges.entrySet()) {
								Text fName = entry.getKey();
								RangeCalculator.RangeBounds rb = entry.getValue();
								node.setFieldName(fName);
								node.setFieldValue(new Text(""));
								node.setLowerBound(rb.getLower());
								node.setUpperBound(rb.getUpper());
								node.setRangeNode(true);
							}
							rangerators.add(node);
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug(("refactor: " + (node.getContents())));
								BooleanLogicIterator.log.debug(((("refactor: " + (node.getLowerBound())) + "  ") + (node.getUpperBound())));
							}
						}else {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug("AND range more than 2 children");
							}
							List<BooleanLogicTreeNode> temp = new ArrayList<BooleanLogicTreeNode>();
							Enumeration<?> e = node.children();
							while (e.hasMoreElements()) {
								BooleanLogicTreeNode c = ((BooleanLogicTreeNode) (e.nextElement()));
								temp.add(c);
							} 
							for (int j = (temp.size()) - 1; j >= 0; j--) {
								BooleanLogicTreeNode c = temp.get(j);
								if (((((c.getType()) == (JexlOperatorConstants.JJTLENODE)) || ((c.getType()) == (JexlOperatorConstants.JJTLTNODE))) || ((c.getType()) == (JexlOperatorConstants.JJTGENODE))) || ((c.getType()) == (JexlOperatorConstants.JJTGTNODE))) {
									c.removeFromParent();
								}
							}
							for (Map.Entry<Text, RangeCalculator.RangeBounds> entry : ranges.entrySet()) {
								Text fName = entry.getKey();
								BooleanLogicTreeNode nchild = new BooleanLogicTreeNode(ParserTreeConstants.JJTORNODE, fName.toString(), "");
								RangeCalculator.RangeBounds rb = entry.getValue();
								nchild.setFieldValue(new Text(""));
								nchild.setLowerBound(rb.getLower());
								nchild.setUpperBound(rb.getUpper());
								nchild.setRangeNode(true);
								node.add(nchild);
								rangerators.add(nchild);
							}
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug(("refactor: " + (node.getContents())));
							}
						}
					}
				}
			}
		}
		return myroot;
	}

	private static boolean canRollUp(BooleanLogicTreeNode parent) {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug(("canRollUp: testing " + (parent.getContents())));
		}
		if ((parent.getChildCount()) < 1) {
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("canRollUp: child count < 1, return false");
			}
			return false;
		}
		Enumeration<?> e = parent.children();
		while (e.hasMoreElements()) {
			BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (e.nextElement()));
			if ((child.getType()) != (ParserTreeConstants.JJTEQNODE)) {
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					BooleanLogicIterator.log.debug((((("canRollUp: child.getType -> " + (ParserTreeConstants.jjtNodeName[child.getType()])) + " int: ") + (child.getType())) + "  return false"));
				}
				return false;
			}
			if (child.isNegated()) {
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					BooleanLogicIterator.log.debug("canRollUp: child.isNegated, return false");
				}
				return false;
			}
			if (child.getFieldValue().toString().contains("*")) {
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					BooleanLogicIterator.log.debug(("canRollUp: child has wildcard: " + (child.getFieldValue())));
				}
				return false;
			}
		} 
		return true;
	}

	public static void showDepthFirstTraversal(BooleanLogicTreeNode root) {
		System.out.println("DepthFirstTraversal");
		Enumeration<?> e = root.depthFirstEnumeration();
		int i = -1;
		while (e.hasMoreElements()) {
			i += 1;
			BooleanLogicTreeNode n = ((BooleanLogicTreeNode) (e.nextElement()));
			System.out.println(((i + " : ") + n));
		} 
	}

	public static void showBreadthFirstTraversal(BooleanLogicTreeNode root) {
		System.out.println("BreadthFirstTraversal");
		BooleanLogicIterator.log.debug("BooleanLogicIterator.showBreadthFirstTraversal()");
		Enumeration<?> e = root.breadthFirstEnumeration();
		int i = -1;
		while (e.hasMoreElements()) {
			i += 1;
			BooleanLogicTreeNode n = ((BooleanLogicTreeNode) (e.nextElement()));
			System.out.println(((i + " : ") + n));
			BooleanLogicIterator.log.debug(((i + " : ") + n));
		} 
	}

	private void splitLeaves(BooleanLogicTreeNode node) {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug("BoolLogic: splitLeaves()");
		}
		positives = new PriorityQueue<BooleanLogicTreeNode>(10, new BooleanLogicIterator.BooleanLogicTreeNodeComparator());
		negatives.clear();
		Enumeration<?> dfe = node.depthFirstEnumeration();
		while (dfe.hasMoreElements()) {
			BooleanLogicTreeNode elem = ((BooleanLogicTreeNode) (dfe.nextElement()));
			if (elem.isLeaf()) {
				if (elem.isNegated()) {
					negatives.add(elem);
				}else {
					positives.add(elem);
				}
			}
		} 
	}

	private void reHeapPriorityQueue(BooleanLogicTreeNode node) {
		positives.clear();
		Enumeration<?> dfe = node.depthFirstEnumeration();
		BooleanLogicTreeNode elem;
		while (dfe.hasMoreElements()) {
			elem = ((BooleanLogicTreeNode) (dfe.nextElement()));
			if ((elem.isLeaf()) && (!(elem.isNegated()))) {
				positives.add(elem);
			}
		} 
	}

	public boolean hasTop() {
		return (topKey) != null;
	}

	public Key getTopKey() {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug(("getTopKey: " + (topKey)));
		}
		return topKey;
	}

	private void setTopKey(Key key) {
		if (((this.overallRange) != null) && (key != null)) {
			if ((overallRange.getEndKey()) != null) {
				if (!(this.overallRange.contains(key))) {
					topKey = null;
					return;
				}
			}
		}
		topKey = key;
	}

	public Value getTopValue() {
		if ((topValue) == null) {
			topValue = new Value(new byte[0]);
		}
		return topValue;
	}

	private void resetNegatives() {
		for (BooleanLogicTreeNode neg : negatives) {
			neg.setTopKey(null);
			neg.setValid(true);
		}
	}

	private String getEventKeyUid(Key k) {
		if ((k == null) || ((k.getColumnFamily()) == null)) {
			return null;
		}else {
			return k.getColumnFamily().toString();
		}
	}

	private String getIndexKeyUid(Key k) {
		try {
			int idx = 0;
			String sKey = k.getColumnQualifier().toString();
			idx = sKey.indexOf("\u0000");
			return sKey.substring((idx + 1));
		} catch (Exception e) {
			return null;
		}
	}

	private Key getOptimizedAdvanceKey() throws IOException {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug("getOptimizedAdvanceKey() called");
		}
		Enumeration<?> bfe = root.breadthFirstEnumeration();
		ArrayList<BooleanLogicTreeNode> bfl = new ArrayList<BooleanLogicTreeNode>();
		while (bfe.hasMoreElements()) {
			BooleanLogicTreeNode node = ((BooleanLogicTreeNode) (bfe.nextElement()));
			if (!(node.isNegated())) {
				node.setAdvanceKey(node.getTopKey());
				node.setDone(false);
				bfl.add(node);
			}
		} 
		for (int i = (bfl.size()) - 1; i >= 0; i--) {
			if ((bfl.get(i).isLeaf()) || (bfl.get(i).isNegated())) {
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					BooleanLogicIterator.log.debug(("leaf, isDone?: " + (bfl.get(i).isDone())));
				}
				continue;
			}
			BooleanLogicTreeNode node = bfl.get(i);
			node.setDone(false);
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug(((("for loop, node: " + node) + " isDone? ") + (node.isDone())));
			}
			if ((node.getType()) == (ParserTreeConstants.JJTANDNODE)) {
				BooleanLogicTreeNode max = null;
				Enumeration<?> children = node.children();
				boolean firstTime = true;
				while (children.hasMoreElements()) {
					BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (children.nextElement()));
					if ((child.isNegated()) || (child.isChildrenAllNegated())) {
						continue;
					}
					if ((child.getAdvanceKey()) == null) {
						BooleanLogicIterator.log.debug(("\tchild does not advance key: " + (child.printNode())));
						node.setDone(true);
						break;
					}else {
						BooleanLogicIterator.log.debug(("\tchild advanceKey: " + (child.getAdvanceKey())));
					}
					if (firstTime) {
						firstTime = false;
						max = child;
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug(("\tAND block, first valid child: " + child));
						}
						continue;
					}
					BooleanLogicIterator.log.debug(("\tAND block, max: " + max));
					BooleanLogicIterator.log.debug(("\tAND block, child: " + child));
					if ((max.getAdvanceKey().getRow().compareTo(child.getAdvanceKey().getRow())) < 0) {
						max = child;
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug("\tAND block, child row greater, new max.");
						}
						continue;
					}
					String uid_max = getEventKeyUid(max.getAdvanceKey());
					String uid_child = getEventKeyUid(child.getAdvanceKey());
					if (BooleanLogicIterator.log.isDebugEnabled()) {
						if (uid_max == null) {
							BooleanLogicIterator.log.debug("\tuid_max is currently null");
						}else {
							BooleanLogicIterator.log.debug(("\tuid_max: " + uid_max));
						}
						if (uid_child == null) {
							BooleanLogicIterator.log.debug("\tuid_child is null");
						}else {
							BooleanLogicIterator.log.debug(("\tuid_child: " + uid_child));
						}
					}
					if ((uid_max != null) && (uid_child != null)) {
						if ((uid_max.compareTo(uid_child)) < 0) {
							max = child;
						}
					}else
						if (uid_child == null) {
							max = child;
							BooleanLogicIterator.log.debug("uid_child is null, we need to grab the next row.");
							break;
						}else {
							BooleanLogicIterator.log.debug(("max is null and child is not, who should we keep? child: " + child));
							break;
						}

				} 
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					BooleanLogicIterator.log.debug(("attemptOptimization: AND with children, max: " + max));
				}
				if (max != null) {
					node.setAdvanceKey(max.getAdvanceKey());
				}else {
					if (BooleanLogicIterator.log.isDebugEnabled()) {
						BooleanLogicIterator.log.debug("AND block finished, max is null");
					}
					node.setDone(true);
				}
			}else
				if ((node.getType()) == (ParserTreeConstants.JJTORNODE)) {
					BooleanLogicTreeNode min = null;
					Enumeration<?> children = node.children();
					boolean firstTime = true;
					int numChildren = node.getChildCount();
					int allChildrenDone = 0;
					while (children.hasMoreElements()) {
						BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (children.nextElement()));
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug(("\tOR block start, child: " + child));
						}
						if ((child.isNegated()) || (child.isChildrenAllNegated())) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug(("\tskip negated child: " + child));
							}
							numChildren -= 1;
							continue;
						}
						if (child.isDone()) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug(("\tchild is done: " + child));
							}
							allChildrenDone += 1;
							if (numChildren == allChildrenDone) {
								if (BooleanLogicIterator.log.isDebugEnabled()) {
									BooleanLogicIterator.log.debug("\tnumChildren==allChildrenDone, setDone & break");
								}
								node.setDone(true);
								break;
							}
						}
						if ((child.getAdvanceKey()) == null) {
							BooleanLogicIterator.log.debug("\tOR child doesn\'t have top or an AdvanceKey");
							continue;
						}
						if (firstTime) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug(((("\tOR block, first valid node, min=child: " + child) + "  advanceKey: ") + (child.getAdvanceKey())));
							}
							firstTime = false;
							min = child;
							continue;
						}
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug(("\tOR block, min: " + min));
							BooleanLogicIterator.log.debug(("\tOR block, child: " + child));
						}
						if ((min.getAdvanceKey().getRow().toString().compareTo(child.getAdvanceKey().getRow().toString())) > 0) {
							min = child;
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug(("\tmin row was greater than child, min=child: " + min));
							}
							continue;
						}else
							if ((min.getAdvanceKey().getRow().compareTo(child.getAdvanceKey().getRow())) < 0) {
								if (BooleanLogicIterator.log.isDebugEnabled()) {
									BooleanLogicIterator.log.debug(("\tmin row less than childs, keep min: " + min));
								}
								continue;
							}else {
								String uid_min = getEventKeyUid(min.getAdvanceKey());
								String uid_child = getEventKeyUid(child.getAdvanceKey());
								if (BooleanLogicIterator.log.isDebugEnabled()) {
									BooleanLogicIterator.log.debug(((("\ttesting uids, uid_min: " + uid_min) + "  uid_child: ") + uid_child));
								}
								if ((uid_min != null) && (uid_child != null)) {
									if ((uid_min.compareTo(uid_child)) > 0) {
										min = child;
										if (BooleanLogicIterator.log.isDebugEnabled()) {
											BooleanLogicIterator.log.debug(("\tuid_min > uid_child, set min to child: " + min));
										}
									}
								}else
									if (uid_min == null) {
										if (BooleanLogicIterator.log.isDebugEnabled()) {
											BooleanLogicIterator.log.debug(("\tuid_min is null, take childs: " + uid_child));
										}
										min = child;
									}

							}

					} 
					if (BooleanLogicIterator.log.isDebugEnabled()) {
						BooleanLogicIterator.log.debug(("attemptOptimization: OR with children, min: " + min));
					}
					if (min != null) {
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug(("OR block, min != null, advanceKey? " + (min.getAdvanceKey())));
						}
						node.setAdvanceKey(min.getAdvanceKey());
					}else {
						BooleanLogicIterator.log.debug(("OR block, min is null..." + min));
						node.setAdvanceKey(null);
						node.setDone(true);
					}
				}else
					if ((node.getType()) == (ParserTreeConstants.JJTJEXLSCRIPT)) {
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug("getOptimizedAdvanceKey, HEAD node");
						}
						BooleanLogicTreeNode child = ((BooleanLogicTreeNode) (node.getFirstChild()));
						if (child.isDone()) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								BooleanLogicIterator.log.debug("Head node's child is done, need to move to the next row");
							}
							Key k = child.getAdvanceKey();
							if (k == null) {
								if (BooleanLogicIterator.log.isDebugEnabled()) {
									BooleanLogicIterator.log.debug("HEAD node, advance key is null, try to grab next row from topKey");
								}
								if (hasTop()) {
									k = this.getTopKey();
									child.setAdvanceKey(new Key(new Text(((k.getRow().toString()) + "\u0001"))));
								}else {
									return null;
								}
							}else {
								Text row = new Text(((k.getRow().toString()) + "\u0001"));
								k = new Key(row);
								child.setAdvanceKey(k);
							}
						}
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							BooleanLogicIterator.log.debug(("advance Key: " + (child.getAdvanceKey())));
						}
						Key key = new Key(child.getAdvanceKey().getRow(), child.getAdvanceKey().getColumnFamily(), child.getAdvanceKey().getColumnFamily());
						return key;
					}


		}
		return null;
	}

	private boolean jump(Key jumpKey) throws IOException {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug("JUMP!");
		}
		Enumeration<?> bfe = root.breadthFirstEnumeration();
		while (bfe.hasMoreElements()) {
			BooleanLogicTreeNode n = ((BooleanLogicTreeNode) (bfe.nextElement()));
			n.setAdvanceKey(null);
		} 
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug(("jump, All leaves need to advance to: " + jumpKey));
		}
		String advanceUid = getIndexKeyUid(jumpKey);
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug(("advanceUid =>  " + advanceUid));
		}
		boolean ok = true;
		for (BooleanLogicTreeNode leaf : positives) {
			leaf.jump(jumpKey);
		}
		return ok;
	}

	@SuppressWarnings("unused")
	public void next() throws IOException {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug("next() method called");
		}
		boolean finished = false;
		boolean ok = true;
		if (positives.isEmpty()) {
			setTopKey(null);
			return;
		}
		Key previousJumpKey = null;
		while (!finished) {
			Key jumpKey = this.getOptimizedAdvanceKey();
			if (jumpKey == null) {
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					BooleanLogicIterator.log.debug("next(), jump key is null, stopping");
				}
				setTopKey(null);
				return;
			}
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				if (jumpKey != null) {
					BooleanLogicIterator.log.debug(("next(), jumpKey: " + jumpKey));
				}else {
					BooleanLogicIterator.log.debug("jumpKey is null");
				}
			}
			boolean same = false;
			if ((jumpKey != null) && ((topKey) != null)) {
				same = getIndexKeyUid(jumpKey).equals(getEventKeyUid(topKey));
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					BooleanLogicIterator.log.debug(((("jumpKeyUid: " + (getIndexKeyUid(jumpKey))) + "  topKeyUid: ") + (getEventKeyUid(topKey))));
				}
			}
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug(("previousJumpKey: " + previousJumpKey));
				BooleanLogicIterator.log.debug(("current JumpKey: " + jumpKey));
			}
			if ((jumpKey != null) && (!(this.overallRange.contains(jumpKey)))) {
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					BooleanLogicIterator.log.debug("jumpKey is outside of range, that means the next key is out of range, stopping");
					BooleanLogicIterator.log.debug(((("jumpKey: " + jumpKey) + " overallRange.endKey: ") + (overallRange.getEndKey())));
				}
				setTopKey(null);
				return;
			}
			boolean previousSame = false;
			if ((previousJumpKey != null) && (jumpKey != null)) {
				previousSame = previousJumpKey.equals(jumpKey);
			}
			if ((((jumpKey != null) && (!same)) && (!previousSame)) && ok) {
				previousJumpKey = jumpKey;
				ok = jump(jumpKey);
				if (testTreeState()) {
					Key tempKey = root.getTopKey();
					if (!(negatives.isEmpty())) {
						advanceNegatives(this.root.getTopKey());
						if (!(testTreeState())) {
							continue;
						}
					}
					if (root.getTopKey().equals(tempKey)) {
						if (BooleanLogicIterator.log.isDebugEnabled()) {
							if (this.root.hasTop()) {
								BooleanLogicIterator.log.debug(("this.root.getTopKey()->" + (this.root.getTopKey())));
							}else {
								BooleanLogicIterator.log.debug("next, this.root.getTopKey() is null");
							}
							if ((topKey) != null) {
								BooleanLogicIterator.log.debug(("topKey->" + (topKey)));
							}else {
								BooleanLogicIterator.log.debug("topKey is null");
							}
						}
						if ((compare(topKey, this.root.getTopKey())) != 0) {
							setTopKey(this.root.getTopKey());
							return;
						}
					}
				}
			}else {
				reHeapPriorityQueue(this.root);
				BooleanLogicTreeNode node;
				while (true) {
					node = positives.poll();
					if ((!(node.isDone())) && (node.hasTop())) {
						break;
					}
					if (positives.isEmpty()) {
						setTopKey(null);
						return;
					}
				} 
				if (BooleanLogicIterator.log.isDebugEnabled()) {
					if (jumpKey == null) {
						BooleanLogicIterator.log.debug("no jump, jumpKey is null");
					}else
						if ((topKey) == null) {
							BooleanLogicIterator.log.debug((("no jump, jumpKey: " + jumpKey) + "  topKey: null"));
						}else {
							BooleanLogicIterator.log.debug(((("no jump, jumpKey: " + jumpKey) + "  topKey: ") + (topKey)));
						}

					BooleanLogicIterator.log.debug(("next, (no jump) min node: " + node));
					BooleanLogicIterator.log.debug(node);
				}
				node.next();
				resetNegatives();
				if (!(node.hasTop())) {
					node.setValid(false);
					if (testTreeState()) {
						if ((topKey.compareTo(this.root.getTopKey())) != 0) {
							if ((this.overallRange) != null) {
								if (this.overallRange.contains(root.getTopKey())) {
									setTopKey(this.root.getTopKey());
									return;
								}else {
									setTopKey(null);
									finished = true;
									return;
								}
							}else {
								setTopKey(this.root.getTopKey());
								return;
							}
						}
					}
				}else {
					if (overallRange.contains(node.getTopKey())) {
						positives.add(node);
					}
					if (testTreeState()) {
						Key tempKey = root.getTopKey();
						if (!(negatives.isEmpty())) {
							advanceNegatives(this.root.getTopKey());
							if (!(testTreeState())) {
								continue;
							}
						}
						if (root.getTopKey().equals(tempKey)) {
							if (BooleanLogicIterator.log.isDebugEnabled()) {
								if (this.root.hasTop()) {
									BooleanLogicIterator.log.debug(("this.root.getTopKey()->" + (this.root.getTopKey())));
								}else {
									BooleanLogicIterator.log.debug("next, this.root.getTopKey() is null");
								}
								if ((topKey) != null) {
									BooleanLogicIterator.log.debug(("topKey->" + (topKey)));
								}else {
									BooleanLogicIterator.log.debug("topKey is null");
								}
							}
							if ((compare(topKey, this.root.getTopKey())) != 0) {
								if ((this.overallRange) != null) {
									if (overallRange.contains(this.root.getTopKey())) {
										setTopKey(this.root.getTopKey());
										return;
									}else {
										topKey = null;
										finished = true;
										return;
									}
								}else {
									setTopKey(this.root.getTopKey());
									return;
								}
							}
						}
					}
				}
				if (positives.isEmpty()) {
					finished = true;
					topKey = null;
				}
			}
		} 
	}

	private void advanceNegatives(Key k) throws IOException {
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug(("advancingNegatives for Key: " + k));
		}
		Text rowID = k.getRow();
		Text colFam = k.getColumnFamily();
		for (BooleanLogicTreeNode neg : negatives) {
			Key startKey = new Key(rowID, neg.getFieldName(), new Text((((neg.getFieldValue()) + "\u0000") + colFam)));
			Key endKey = new Key(rowID, neg.getFieldName(), new Text(((((neg.getFieldValue()) + "\u0000") + colFam) + "\u0001")));
			Range range = new Range(startKey, true, endKey, false);
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug(("range: " + range));
			}
			neg.seek(range, BooleanLogicIterator.EMPTY_COL_FAMS, false);
			if (neg.hasTop()) {
				neg.setValid(false);
			}
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				if (neg.hasTop()) {
					BooleanLogicIterator.log.debug(("neg top key: " + (neg.getTopKey())));
				}else {
					BooleanLogicIterator.log.debug("neg has no top");
				}
			}
		}
	}

	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		this.overallRange = range;
		if (BooleanLogicIterator.log.isDebugEnabled()) {
			BooleanLogicIterator.log.debug(("seek, overallRange: " + (overallRange)));
		}
		topKey = null;
		root.setTopKey(null);
		setupRangerators(range);
		reHeapPriorityQueue(this.root);
		for (BooleanLogicTreeNode node : positives) {
			node.setDone(false);
			node.seek(range, columnFamilies, inclusive);
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				String tk = "empty";
				if (node.hasTop()) {
					tk = node.getTopKey().toString();
				}
				BooleanLogicIterator.log.debug(((("leaf: " + (node.getContents())) + " topKey: ") + tk));
			}
		}
		splitLeaves(this.root);
		resetNegatives();
		if ((testTreeState()) && (overallRange.contains(root.getTopKey()))) {
			if (!(negatives.isEmpty())) {
				advanceNegatives(this.root.getTopKey());
				if (!(testTreeState())) {
					next();
				}
			}
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug(((((("overallRange " + (overallRange)) + " topKey ") + (this.root.getTopKey())) + " contains ") + (overallRange.contains(this.root.getTopKey()))));
			}
			if ((overallRange.contains(this.root.getTopKey())) && (this.root.isValid())) {
				setTopKey(this.root.getTopKey());
			}else {
				setTopKey(null);
				return;
			}
		}else {
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				BooleanLogicIterator.log.debug("seek, testTreeState is false, HEAD(root) does not have top");
			}
			List<BooleanLogicTreeNode> removals = new ArrayList<BooleanLogicTreeNode>();
			for (BooleanLogicTreeNode node : positives) {
				if ((!(node.hasTop())) || (!(overallRange.contains(node.getTopKey())))) {
					removals.add(node);
				}
			}
			for (BooleanLogicTreeNode node : removals) {
				positives.remove(node);
			}
			next();
			return;
		}
	}

	private int compare(Key k1, Key k2) {
		if ((k1 != null) && (k2 != null)) {
			return k1.compareTo(k2);
		}else
			if ((k1 == null) && (k2 == null)) {
				return 0;
			}else
				if (k1 == null) {
					return 1;
				}else {
					return -1;
				}


	}

	private void setupRangerators(Range range) throws IOException {
		if (((rangerators) == null) || (rangerators.isEmpty())) {
			return;
		}
		for (BooleanLogicTreeNode node : rangerators) {
			Set<String> fValues = new HashSet<String>();
			OrIterator orIter = new OrIterator();
			SortedKeyValueIterator<Key, Value> siter = sourceIterator.deepCopy(BooleanLogicIterator.env);
			UniqFieldNameValueIterator uniq = new UniqFieldNameValueIterator(node.getFieldName(), node.getLowerBound(), node.getUpperBound());
			uniq.setSource(siter);
			uniq.seek(range, BooleanLogicIterator.EMPTY_COL_FAMS, false);
			while (uniq.hasTop()) {
				Key k = uniq.getTopKey();
				keyParser.parse(k);
				String val = keyParser.getFieldValue();
				if (!(fValues.contains(val))) {
					fValues.add(val);
					orIter.addTerm(siter, node.getFieldName(), new Text(val), BooleanLogicIterator.env);
					if (BooleanLogicIterator.log.isDebugEnabled()) {
						BooleanLogicIterator.log.debug(((("setupRangerators, adding to OR:  " + (node.getFieldName())) + ":") + val));
					}
				}else {
					BooleanLogicIterator.log.debug(("already have this one: " + val));
				}
				uniq.next();
			} 
			node.setUserObject(orIter);
		}
	}

	public class BooleanLogicTreeNodeComparator implements Comparator<Object> {
		public int compare(Object o1, Object o2) {
			BooleanLogicTreeNode n1 = ((BooleanLogicTreeNode) (o1));
			BooleanLogicTreeNode n2 = ((BooleanLogicTreeNode) (o2));
			Key k1 = n1.getTopKey();
			Key k2 = n2.getTopKey();
			if (BooleanLogicIterator.log.isDebugEnabled()) {
				String t1 = "null";
				String t2 = "null";
				if (k1 != null) {
					t1 = ((k1.getRow().toString()) + "\u0000") + (k1.getColumnFamily().toString());
				}
				if (k2 != null) {
					t2 = ((k2.getRow().toString()) + "\u0000") + (k2.getColumnFamily().toString());
				}
				BooleanLogicIterator.log.debug(((("BooleanLogicTreeNodeComparator   \tt1: " + t1) + "  t2: ") + t2));
			}
			if ((k1 != null) && (k2 != null)) {
				return k1.compareTo(k2);
			}else
				if ((k1 == null) && (k2 == null)) {
					return 0;
				}else
					if (k1 == null) {
						return 1;
					}else {
						return -1;
					}


		}
	}

	public OptionDescriber.IteratorOptions describeOptions() {
		return new OptionDescriber.IteratorOptions(getClass().getSimpleName(), "evaluates event objects against an expression", Collections.singletonMap(BooleanLogicIterator.QUERY_OPTION, "query expression"), null);
	}

	public boolean validateOptions(Map<String, String> options) {
		if (!(options.containsKey(BooleanLogicIterator.QUERY_OPTION))) {
			return false;
		}
		if (!(options.containsKey(BooleanLogicIterator.FIELD_INDEX_QUERY))) {
			return false;
		}
		this.updatedQuery = options.get(BooleanLogicIterator.FIELD_INDEX_QUERY);
		return true;
	}
}


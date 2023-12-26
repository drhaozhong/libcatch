package org.apache.accumulo.examples.wikisearch.iterator;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.examples.wikisearch.function.QueryFunctions;
import org.apache.accumulo.examples.wikisearch.util.FieldIndexKeyParser;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class FieldIndexIterator extends WrappingIterator {
	private Key topKey = null;

	private Value topValue = null;

	private Range range = null;

	private Text currentRow;

	private Text fName = null;

	private String fNameString = null;

	private Text fValue = null;

	private String fOperator = null;

	private Expression expr = null;

	private static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

	protected static final Logger log = Logger.getLogger(FieldIndexIterator.class);

	private boolean negated = false;

	private int type;

	private static final String NULL_BYTE = "\u0000";

	private static final String ONE_BYTE = "\u0001";

	private static JexlEngine engine = new JexlEngine();

	private Range parentRange;

	private Text parentEndRow = null;

	private FieldIndexKeyParser keyParser;

	static {
		FieldIndexIterator.engine.setCache(128);
		Map<String, Object> functions = new HashMap<String, Object>();
		functions.put("f", QueryFunctions.class);
		FieldIndexIterator.engine.setFunctions(functions);
	}

	public static void setLogLevel(Level l) {
		FieldIndexIterator.log.setLevel(l);
	}

	public FieldIndexIterator() {
	}

	public FieldIndexIterator(int type, Text rowId, Text fieldName, Text fieldValue, String operator) {
		this.fName = fieldName;
		this.fNameString = fName.toString().substring(3);
		this.fValue = fieldValue;
		this.fOperator = operator;
		this.range = buildRange(rowId);
		this.negated = false;
		this.type = type;
		StringBuilder buf = new StringBuilder();
		buf.append(fNameString).append(" ").append(this.fOperator).append(" ").append("'").append(fValue.toString()).append("'");
		this.expr = FieldIndexIterator.engine.createExpression(buf.toString());
		keyParser = createDefaultKeyParser();
	}

	public FieldIndexIterator(int type, Text rowId, Text fieldName, Text fieldValue, boolean neg, String operator) {
		this.fName = fieldName;
		this.fNameString = fName.toString().substring(3);
		this.fValue = fieldValue;
		this.fOperator = operator;
		this.range = buildRange(rowId);
		this.negated = neg;
		this.type = type;
		StringBuilder buf = new StringBuilder();
		buf.append(fNameString).append(" ").append(this.fOperator).append(" ").append("'").append(fValue.toString()).append("'");
		this.expr = FieldIndexIterator.engine.createExpression(buf.toString());
		keyParser = createDefaultKeyParser();
	}

	public FieldIndexIterator(FieldIndexIterator other, IteratorEnvironment env) {
		setSource(other.getSource().deepCopy(env));
		keyParser = createDefaultKeyParser();
	}

	private FieldIndexKeyParser createDefaultKeyParser() {
		FieldIndexKeyParser parser = new FieldIndexKeyParser();
		return parser;
	}

	@Override
	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new FieldIndexIterator(this, env);
	}

	@Override
	public Key getTopKey() {
		return topKey;
	}

	@Override
	public Value getTopValue() {
		return topValue;
	}

	@Override
	public boolean hasTop() {
		return (topKey) != null;
	}

	@Override
	public void next() throws IOException {
		if (FieldIndexIterator.log.isDebugEnabled()) {
			FieldIndexIterator.log.debug("next()");
		}
		if (this.hasTop()) {
			currentRow = topKey.getRow();
		}
		getSource().next();
		while (true) {
			FieldIndexIterator.log.debug(("next(), Range: " + (range)));
			if (getSource().hasTop()) {
				Key k = getSource().getTopKey();
				if (range.contains(k)) {
					if (matches(k)) {
						topKey = k;
						topValue = getSource().getTopValue();
						return;
					}else {
						getSource().next();
					}
				}else {
					if ((parentEndRow) != null) {
						if (k.getRow().equals(currentRow)) {
							currentRow = getNextRow();
						}else
							if (((currentRow) == null) || ((k.getRow().compareTo(currentRow)) > 0)) {
								currentRow = k.getRow();
							}

						if (((currentRow) == null) || ((parentEndRow.compareTo(currentRow)) < 0)) {
							topKey = null;
							topValue = null;
							return;
						}
					}else {
						if (k.getRow().equals(currentRow)) {
							currentRow = getNextRow();
							if ((currentRow) == null) {
								topKey = null;
								topValue = null;
								return;
							}
						}else
							if (((currentRow) == null) || ((k.getRow().compareTo(currentRow)) > 0)) {
								currentRow = k.getRow();
							}

					}
					range = buildRange(currentRow);
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug(("next, range: " + (range)));
					}
					getSource().seek(range, FieldIndexIterator.EMPTY_COL_FAMS, false);
				}
			}else {
				topKey = null;
				topValue = null;
				return;
			}
		} 
	}

	@Override
	public void seek(Range r, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		parentRange = r;
		if (FieldIndexIterator.log.isDebugEnabled()) {
			FieldIndexIterator.log.debug(("begin seek, range: " + r));
		}
		if ((parentRange.getEndKey()) != null) {
			if ((parentRange.getEndKey().getRow()) != null) {
				parentEndRow = parentRange.getEndKey().getRow();
				if (FieldIndexIterator.log.isDebugEnabled()) {
					FieldIndexIterator.log.debug(("begin seek, parentEndRow: " + (parentEndRow)));
				}
			}
		}
		try {
			if (isNegated()) {
				range = r;
				if (FieldIndexIterator.log.isDebugEnabled()) {
					FieldIndexIterator.log.debug("seek, negation, skipping range modification.");
				}
			}else {
				if ((r.getStartKey()) != null) {
					if (((r.getStartKey().getRow()) == null) || (r.getStartKey().getRow().toString().isEmpty())) {
						currentRow = getFirstRow();
					}else {
						currentRow = r.getStartKey().getRow();
					}
					this.range = buildRange(currentRow);
				}else {
					currentRow = getFirstRow();
					this.range = buildRange(currentRow);
				}
			}
			setTopKey(null);
			setTopValue(null);
			if (FieldIndexIterator.log.isDebugEnabled()) {
				FieldIndexIterator.log.debug(("seek, incoming range: " + (range)));
			}
			getSource().seek(range, columnFamilies, inclusive);
			while ((topKey) == null) {
				if (getSource().hasTop()) {
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug(("seek, source has top: " + (getSource().getTopKey())));
					}
					Key k = getSource().getTopKey();
					if (range.contains(k)) {
						if (matches(k)) {
							topKey = k;
							topValue = getSource().getTopValue();
							if (FieldIndexIterator.log.isDebugEnabled()) {
								FieldIndexIterator.log.debug("seek, source has top in valid range");
							}
						}else {
							getSource().next();
						}
					}else {
						if (FieldIndexIterator.log.isDebugEnabled()) {
							FieldIndexIterator.log.debug("seek, top out of range");
							String pEndRow = "empty";
							if ((parentEndRow) != null) {
								pEndRow = parentEndRow.toString();
							}
							FieldIndexIterator.log.debug(((((("source.topKey.row: " + (k.getRow())) + "\t currentRow: ") + (currentRow)) + "\t parentEndRow: ") + pEndRow));
						}
						if (isNegated()) {
							topKey = null;
							topValue = null;
							return;
						}
						if ((parentEndRow) != null) {
							if (k.getRow().equals(currentRow)) {
								currentRow = getNextRow();
							}
							if (((currentRow) == null) || ((parentEndRow.compareTo(currentRow)) < 0)) {
								topKey = null;
								topValue = null;
								return;
							}
						}else {
							if (k.getRow().equals(currentRow)) {
								currentRow = getNextRow();
								if ((currentRow) == null) {
									topKey = null;
									topValue = null;
									return;
								}
							}
						}
						range = buildRange(currentRow);
						if (FieldIndexIterator.log.isDebugEnabled()) {
							FieldIndexIterator.log.debug(("currentRow: " + (currentRow)));
							FieldIndexIterator.log.debug(("seek, range: " + (range)));
						}
						getSource().seek(range, columnFamilies, inclusive);
					}
				}else {
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug("seek, underlying source had no top key.");
					}
					topKey = null;
					topValue = null;
					return;
				}
			} 
			if (FieldIndexIterator.log.isDebugEnabled()) {
				FieldIndexIterator.log.debug(("seek, topKey found: " + (topKey)));
			}
		} catch (IOException e) {
			topKey = null;
			topValue = null;
			throw new IOException();
		}
	}

	public boolean isNegated() {
		return negated;
	}

	public Text getCurrentRow() {
		return currentRow;
	}

	public Text getfName() {
		return fName;
	}

	public Text getfValue() {
		return fValue;
	}

	public boolean jump(Key jumpKey) throws IOException {
		if (FieldIndexIterator.log.isDebugEnabled()) {
			String pEndRow = "empty";
			if ((parentEndRow) != null) {
				pEndRow = parentEndRow.toString();
			}
			FieldIndexIterator.log.debug(((("jump, current range: " + (range)) + "  parentEndRow is: ") + pEndRow));
		}
		if (((parentEndRow) != null) && ((jumpKey.getRow().compareTo(parentEndRow)) > 0)) {
			if (FieldIndexIterator.log.isDebugEnabled()) {
				FieldIndexIterator.log.debug(((("jumpRow: " + (jumpKey.getRow())) + " is greater than my parentEndRow: ") + (parentEndRow)));
			}
			return false;
		}
		int comp;
		if (!(this.hasTop())) {
			if (FieldIndexIterator.log.isDebugEnabled()) {
				FieldIndexIterator.log.debug(("current row: " + (this.currentRow)));
			}
			if ((parentEndRow) != null) {
				if ((jumpKey.getRow().compareTo(parentEndRow)) > 0) {
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug("jumpKey row is greater than my parentEndRow, done");
					}
					return false;
				}
				if ((currentRow) == null) {
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug("I have parentEndRow, but no current row, must have hit end of tablet, done");
					}
					return false;
				}
				if ((currentRow.compareTo(jumpKey.getRow())) >= 0) {
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug("I have parentEndRow, but topKey, and my currentRow is >= jumpRow, done");
					}
					return false;
				}
			}else {
				if ((currentRow) == null) {
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug("no parentEndRow and current Row is null, must have hit end of tablet, done");
					}
					return false;
				}
				if ((currentRow.compareTo(jumpKey.getRow())) >= 0) {
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug("no parentEndRow, no topKey, and currentRow is >= jumpRow, done");
					}
					return false;
				}
			}
			if (FieldIndexIterator.log.isDebugEnabled()) {
				FieldIndexIterator.log.debug("no topKey, but jumpRow is ahead and I'm allowed to go to it, marking");
			}
			comp = -1;
		}else {
			if (FieldIndexIterator.log.isDebugEnabled()) {
				FieldIndexIterator.log.debug("have top, can do normal comparisons");
			}
			comp = this.topKey.getRow().compareTo(jumpKey.getRow());
		}
		if (comp > 0) {
			if (canBeInNextRow()) {
				if (FieldIndexIterator.log.isDebugEnabled()) {
					FieldIndexIterator.log.debug("I'm ahead of jump row & it's ok.");
					FieldIndexIterator.log.debug(((((("jumpRow: " + (jumpKey.getRow())) + " myRow: ") + (topKey.getRow())) + " parentEndRow: ") + (parentEndRow)));
				}
				return true;
			}else {
				if (FieldIndexIterator.log.isDebugEnabled()) {
					FieldIndexIterator.log.debug("I'm ahead of jump row & can't be here, or at end of tablet.");
				}
				topKey = null;
				topValue = null;
				return false;
			}
		}else
			if (comp < 0) {
				if (FieldIndexIterator.log.isDebugEnabled()) {
					String myRow = "";
					if (hasTop()) {
						myRow = topKey.getRow().toString();
					}else
						if ((currentRow) != null) {
							myRow = currentRow.toString();
						}

					FieldIndexIterator.log.debug((((("My row " + myRow) + " is less than jump row: ") + (jumpKey.getRow())) + " seeking"));
				}
				range = buildRange(jumpKey.getRow());
				boolean success = jumpSeek(range);
				if ((FieldIndexIterator.log.isDebugEnabled()) && success) {
					FieldIndexIterator.log.debug(("uid forced jump, found topKey: " + (topKey)));
				}
				if (!(this.hasTop())) {
					FieldIndexIterator.log.debug("seeked with new row and had no top");
					topKey = null;
					topValue = null;
					return false;
				}else
					if (((parentEndRow) != null) && ((currentRow.compareTo(parentEndRow)) > 0)) {
						if (FieldIndexIterator.log.isDebugEnabled()) {
							FieldIndexIterator.log.debug(((("myRow: " + (getTopKey().getRow())) + " is past parentEndRow: ") + (parentEndRow)));
						}
						topKey = null;
						topValue = null;
						return false;
					}

				if (FieldIndexIterator.log.isDebugEnabled()) {
					FieldIndexIterator.log.debug(("jumped, valid top: " + (getTopKey())));
				}
				return true;
			}else {
				keyParser.parse(topKey);
				String myUid = keyParser.getUid();
				keyParser.parse(jumpKey);
				String jumpUid = keyParser.getUid();
				int ucomp = myUid.compareTo(jumpUid);
				if (FieldIndexIterator.log.isDebugEnabled()) {
					FieldIndexIterator.log.debug(((((("topKeyUid: " + myUid) + "  jumpUid: ") + jumpUid) + "  myUid.compareTo(jumpUid)->") + ucomp));
				}
				if (ucomp < 0) {
					FieldIndexIterator.log.debug(((("my uid is less than jumpUid, topUid: " + myUid) + "   jumpUid: ") + jumpUid));
					Text cq = jumpKey.getColumnQualifier();
					int index = cq.find(FieldIndexIterator.NULL_BYTE);
					if (0 <= index) {
						cq.set(cq.getBytes(), (index + 1), (((cq.getLength()) - index) - 1));
					}else {
						FieldIndexIterator.log.error("Expected a NULL separator in the column qualifier");
						this.topKey = null;
						this.topValue = null;
						return false;
					}
					Key startKey = new Key(topKey.getRow(), fName, new Text((((fValue) + (FieldIndexIterator.NULL_BYTE)) + cq)));
					Key endKey = new Key(topKey.getRow(), fName, new Text(((fValue) + (FieldIndexIterator.ONE_BYTE))));
					range = new Range(startKey, true, endKey, false);
					FieldIndexIterator.log.debug((("Using range: " + (range)) + " to seek"));
					boolean success = jumpSeek(range);
					if ((FieldIndexIterator.log.isDebugEnabled()) && success) {
						FieldIndexIterator.log.debug(("uid forced jump, found topKey: " + (topKey)));
					}
					return success;
				}else {
					FieldIndexIterator.log.debug(((("my uid is greater than jumpUid, topKey: " + (topKey)) + "   jumpKey: ") + jumpKey));
					FieldIndexIterator.log.debug("doing nothing");
				}
			}

		return hasTop();
	}

	private void setTopKey(Key key) {
		topKey = key;
	}

	private void setTopValue(Value v) {
		this.topValue = v;
	}

	private boolean canBeInNextRow() {
		if ((parentEndRow) == null) {
			return true;
		}else
			if ((currentRow) == null) {
				return false;
			}else
				if ((currentRow.compareTo(parentEndRow)) <= 0) {
					return true;
				}else {
					return false;
				}


	}

	private Range buildRange(Text rowId) {
		if (((((((type) == (ParserTreeConstants.JJTGTNODE)) || ((type) == (ParserTreeConstants.JJTGENODE))) || ((type) == (ParserTreeConstants.JJTLTNODE))) || ((type) == (ParserTreeConstants.JJTLENODE))) || ((type) == (ParserTreeConstants.JJTERNODE))) || ((type) == (ParserTreeConstants.JJTNRNODE))) {
			Key startKey = new Key(rowId, fName);
			Key endKey = new Key(rowId, new Text(((fName) + (FieldIndexIterator.NULL_BYTE))));
			return new Range(startKey, true, endKey, false);
		}else {
			Key startKey = new Key(rowId, fName, new Text(((fValue) + (FieldIndexIterator.NULL_BYTE))));
			Key endKey = new Key(rowId, fName, new Text(((fValue) + (FieldIndexIterator.ONE_BYTE))));
			return new Range(startKey, true, endKey, false);
		}
	}

	private Text getNextRow() throws IOException {
		if (FieldIndexIterator.log.isDebugEnabled()) {
			FieldIndexIterator.log.debug("getNextRow()");
		}
		Key fakeKey = new Key(new Text(((currentRow) + (FieldIndexIterator.NULL_BYTE))));
		Range fakeRange = new Range(fakeKey, fakeKey);
		getSource().seek(fakeRange, FieldIndexIterator.EMPTY_COL_FAMS, false);
		if (getSource().hasTop()) {
			return getSource().getTopKey().getRow();
		}else {
			return null;
		}
	}

	private Text getFirstRow() throws IOException {
		getSource().seek(new Range(), FieldIndexIterator.EMPTY_COL_FAMS, false);
		if (getSource().hasTop()) {
			return getSource().getTopKey().getRow();
		}else {
			throw new IOException();
		}
	}

	private boolean matches(Key k) {
		if (FieldIndexIterator.log.isDebugEnabled()) {
			FieldIndexIterator.log.debug("You've reached the match function!");
		}
		JexlContext ctx = new MapContext();
		keyParser.parse(k);
		String fieldValue = keyParser.getFieldValue();
		ctx.set(fNameString, fieldValue);
		Object o = expr.evaluate(ctx);
		if ((o instanceof Boolean) && (((Boolean) (o)) == true)) {
			if (FieldIndexIterator.log.isDebugEnabled()) {
				FieldIndexIterator.log.debug(((((((("matches:: fName: " + (fName)) + " , fValue: ") + fieldValue) + " ,  operator: ") + (fOperator)) + " , key: ") + k));
			}
			return true;
		}else {
			if (FieldIndexIterator.log.isDebugEnabled()) {
				FieldIndexIterator.log.debug(((((((("NO MATCH:: fName: " + (fName)) + " , fValue: ") + fieldValue) + " ,  operator: ") + (fOperator)) + " , key: ") + k));
			}
			return false;
		}
	}

	private boolean jumpSeek(Range r) throws IOException {
		range = r;
		setTopKey(null);
		setTopValue(null);
		getSource().seek(range, FieldIndexIterator.EMPTY_COL_FAMS, false);
		while ((topKey) == null) {
			if (getSource().hasTop()) {
				if (FieldIndexIterator.log.isDebugEnabled()) {
					FieldIndexIterator.log.debug(("jump, source has top: " + (getSource().getTopKey())));
				}
				Key k = getSource().getTopKey();
				if (range.contains(k)) {
					if (matches(k)) {
						topKey = k;
						topValue = getSource().getTopValue();
						if (FieldIndexIterator.log.isDebugEnabled()) {
							FieldIndexIterator.log.debug("jump, source has top in valid range");
						}
					}else {
						getSource().next();
					}
				}else {
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug("jump, top out of range");
						String pEndRow = "empty";
						if ((parentEndRow) != null) {
							pEndRow = parentEndRow.toString();
						}
						FieldIndexIterator.log.debug(((((("source.topKey.row: " + (k.getRow())) + "\t currentRow: ") + (currentRow)) + "\t parentEndRow: ") + pEndRow));
					}
					if ((parentEndRow) != null) {
						if ((currentRow) == null) {
							topKey = null;
							topValue = null;
							return false;
						}
						if (k.getRow().equals(currentRow)) {
							currentRow = getNextRow();
						}else
							if ((k.getRow().compareTo(currentRow)) > 0) {
								currentRow = k.getRow();
							}

						if (((currentRow) == null) || ((parentEndRow.compareTo(currentRow)) < 0)) {
							topKey = null;
							topValue = null;
							return false;
						}
					}else {
						if (((currentRow) == null) || ((k.getRow()) == null)) {
							topKey = null;
							topValue = null;
							return false;
						}
						if (k.getRow().equals(currentRow)) {
							currentRow = getNextRow();
							if ((currentRow) == null) {
								topKey = null;
								topValue = null;
								return false;
							}
						}else
							if ((k.getRow().compareTo(currentRow)) > 0) {
								currentRow = k.getRow();
							}

					}
					range = buildRange(currentRow);
					if (FieldIndexIterator.log.isDebugEnabled()) {
						FieldIndexIterator.log.debug(("jump, new build range: " + (range)));
					}
					getSource().seek(range, FieldIndexIterator.EMPTY_COL_FAMS, false);
				}
			}else {
				if (FieldIndexIterator.log.isDebugEnabled()) {
					FieldIndexIterator.log.debug("jump, underlying source had no top key.");
				}
				topKey = null;
				topValue = null;
				return false;
			}
		} 
		return hasTop();
	}
}


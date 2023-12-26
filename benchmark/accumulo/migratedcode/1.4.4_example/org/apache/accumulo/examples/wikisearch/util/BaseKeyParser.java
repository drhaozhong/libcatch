package org.apache.accumulo.examples.wikisearch.util;


import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;


public class BaseKeyParser {
	public static final String ROW_FIELD = "row";

	public static final String COLUMN_FAMILY_FIELD = "columnFamily";

	public static final String COLUMN_QUALIFIER_FIELD = "columnQualifier";

	protected Map<String, String> keyFields = new HashMap<String, String>();

	protected Key key = null;

	public void parse(Key key) {
		this.key = key;
		keyFields.clear();
		keyFields.put(BaseKeyParser.ROW_FIELD, key.getRow().toString());
		keyFields.put(BaseKeyParser.COLUMN_FAMILY_FIELD, key.getColumnFamily().toString());
		keyFields.put(BaseKeyParser.COLUMN_QUALIFIER_FIELD, key.getColumnQualifier().toString());
	}

	public String getFieldValue(String fieldName) {
		return keyFields.get(fieldName);
	}

	public String[] getFieldNames() {
		String[] fieldNames = new String[keyFields.size()];
		return keyFields.keySet().toArray(fieldNames);
	}

	public BaseKeyParser duplicate() {
		return new BaseKeyParser();
	}

	public String getRow() {
		return keyFields.get(BaseKeyParser.ROW_FIELD);
	}

	public String getColumnFamily() {
		return keyFields.get(BaseKeyParser.COLUMN_FAMILY_FIELD);
	}

	public String getColumnQualifier() {
		return keyFields.get(BaseKeyParser.COLUMN_QUALIFIER_FIELD);
	}

	public Key getKey() {
		return this.key;
	}
}


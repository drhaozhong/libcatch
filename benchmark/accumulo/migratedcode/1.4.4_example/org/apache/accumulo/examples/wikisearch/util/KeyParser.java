package org.apache.accumulo.examples.wikisearch.util;


import java.util.Map;
import org.apache.accumulo.core.data.Key;


public class KeyParser extends BaseKeyParser {
	public static final String SELECTOR_FIELD = "selector";

	public static final String DATATYPE_FIELD = "dataType";

	public static final String FIELDNAME_FIELD = "fieldName";

	public static final String UID_FIELD = "uid";

	public static final String DELIMITER = "\u0000";

	@Override
	public void parse(Key key) {
		super.parse(key);
		String[] colFamParts = this.keyFields.get(BaseKeyParser.COLUMN_FAMILY_FIELD).split(KeyParser.DELIMITER);
		this.keyFields.put(KeyParser.FIELDNAME_FIELD, ((colFamParts.length) >= 2 ? colFamParts[1] : ""));
		String[] colQualParts = this.keyFields.get(BaseKeyParser.COLUMN_QUALIFIER_FIELD).split(KeyParser.DELIMITER);
		this.keyFields.put(KeyParser.SELECTOR_FIELD, ((colQualParts.length) >= 1 ? colQualParts[0] : ""));
		this.keyFields.put(KeyParser.DATATYPE_FIELD, ((colQualParts.length) >= 2 ? colQualParts[1] : ""));
		this.keyFields.put(KeyParser.UID_FIELD, ((colQualParts.length) >= 3 ? colQualParts[2] : ""));
	}

	@Override
	public BaseKeyParser duplicate() {
		return new KeyParser();
	}

	public String getSelector() {
		return keyFields.get(KeyParser.SELECTOR_FIELD);
	}

	public String getDataType() {
		return keyFields.get(KeyParser.DATATYPE_FIELD);
	}

	public String getFieldName() {
		return keyFields.get(KeyParser.FIELDNAME_FIELD);
	}

	public String getUid() {
		return keyFields.get(KeyParser.UID_FIELD);
	}

	public String getDataTypeUid() {
		return ((getDataType()) + (KeyParser.DELIMITER)) + (getUid());
	}

	public String getFieldValue() {
		return getSelector();
	}
}


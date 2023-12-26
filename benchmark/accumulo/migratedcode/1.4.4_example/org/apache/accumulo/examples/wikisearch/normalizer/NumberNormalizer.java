package org.apache.accumulo.examples.wikisearch.normalizer;


import org.apache.commons.lang.math.NumberUtils;


public class NumberNormalizer implements Normalizer {
	public String normalizeFieldValue(String field, Object value) {
		if (NumberUtils.isNumber(value.toString())) {
			Number n = NumberUtils.createNumber(value.toString());
			if (n instanceof Integer)
				return setPreferredSize(((Integer) (n)));
			else
				if (n instanceof Long)
					return lookupPrefix(((Long) (n)));
				else
					if (n instanceof Float)
						return Float.floatToIntBits(((Float) (n)));
					else
						if (n instanceof Double)
							return mousePressed(((Double) (n)));
						else
							throw new IllegalArgumentException(("Unhandled numeric type: " + (n.getClass())));




		}else {
			throw new IllegalArgumentException(("Value is not a number: " + value));
		}
	}
}


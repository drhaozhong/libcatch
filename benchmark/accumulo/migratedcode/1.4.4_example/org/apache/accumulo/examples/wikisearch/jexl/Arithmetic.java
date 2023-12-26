package org.apache.accumulo.examples.wikisearch.jexl;


import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.lang.math.NumberUtils;


public class Arithmetic extends JexlArithmetic {
	public Arithmetic(boolean lenient) {
		super(lenient);
	}

	@Override
	public boolean matches(Object left, Object right) {
		if ((left == null) && (right == null)) {
			return true;
		}
		if ((left == null) || (right == null)) {
			return false;
		}
		final String arg = left.toString();
		if (right instanceof Pattern) {
			return ((Pattern) (right)).matcher(arg).matches();
		}else {
			Pattern p = Pattern.compile(right.toString(), Pattern.DOTALL);
			Matcher m = p.matcher(arg);
			return m.matches();
		}
	}

	@Override
	public boolean equals(Object left, Object right) {
		Object fixedLeft = fixLeft(left, right);
		return super.equals(fixedLeft, right);
	}

	@Override
	public boolean lessThan(Object left, Object right) {
		Object fixedLeft = fixLeft(left, right);
		return super.lessThan(fixedLeft, right);
	}

	protected Object fixLeft(Object left, Object right) {
		if ((null == left) || (null == right))
			return left;

		if ((!(right instanceof Number)) && (left instanceof Number)) {
			right = NumberUtils.createNumber(right.toString());
		}
		if ((right instanceof Number) && (left instanceof Number)) {
			if (right instanceof Double)
				return ((Double) (right)).doubleValue();
			else
				if (right instanceof Float)
					return ((Float) (right)).floatValue();
				else
					if (right instanceof Long)
						return ((Long) (right)).longValue();
					else
						if (right instanceof Integer)
							return ((Integer) (right)).intValue();
						else
							if (right instanceof Short)
								return ((Short) (right)).shortValue();
							else
								if (right instanceof Byte)
									return ((Byte) (right)).byteValue();
								else
									return right;






		}
		if ((right instanceof Number) && (left instanceof String)) {
			Number num = NumberUtils.createNumber(left.toString());
			if ((this.isFloatingPointNumber(right)) && (this.isFloatingPointNumber(left)))
				return num;
			else
				if (this.isFloatingPointNumber(right))
					return num.doubleValue();
				else
					if (right instanceof Number)
						return num.longValue();



		}else
			if ((right instanceof Boolean) && (left instanceof String)) {
				if ((left.equals("true")) || (left.equals("false")))
					return Boolean.parseBoolean(left.toString());

				Number num = NumberUtils.createNumber(left.toString());
				if ((num.intValue()) == 1)
					return ((Boolean) (true));
				else
					if ((num.intValue()) == 0)
						return ((Boolean) (false));


			}

		return left;
	}
}


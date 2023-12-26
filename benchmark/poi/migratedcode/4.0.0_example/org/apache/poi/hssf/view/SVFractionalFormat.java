package org.apache.poi.hssf.view;


import java.text.FieldPosition;
import java.text.Format;
import java.text.ParseException;
import java.text.ParsePosition;


public class SVFractionalFormat extends Format {
	private short ONE_DIGIT = 1;

	private short TWO_DIGIT = 2;

	private short THREE_DIGIT = 3;

	private short UNITS = 4;

	private int units = 1;

	private short mode = -1;

	public SVFractionalFormat(String formatStr) {
		if ("# ?/?".equals(formatStr))
			mode = ONE_DIGIT;
		else
			if ("# ??/??".equals(formatStr))
				mode = TWO_DIGIT;
			else
				if ("# ???/???".equals(formatStr))
					mode = THREE_DIGIT;
				else
					if ("# ?/2".equals(formatStr)) {
						mode = UNITS;
						units = 2;
					}else
						if ("# ?/4".equals(formatStr)) {
							mode = UNITS;
							units = 4;
						}else
							if ("# ?/8".equals(formatStr)) {
								mode = UNITS;
								units = 8;
							}else
								if ("# ?/16".equals(formatStr)) {
									mode = UNITS;
									units = 16;
								}else
									if ("# ?/10".equals(formatStr)) {
										mode = UNITS;
										units = 10;
									}else
										if ("# ?/100".equals(formatStr)) {
											mode = UNITS;
											units = 100;
										}








	}

	private String format(final double f, final int MaxDen) {
		long Whole = ((long) (f));
		int sign = 1;
		if (f < 0) {
			sign = -1;
		}
		double Precision = 1.0E-5;
		double AllowedError = Precision;
		double d = Math.abs(f);
		d -= Whole;
		double Frac = d;
		double Diff = Frac;
		long Num = 1;
		long Den = 0;
		long A = 0;
		long B = 0;
		long i = 0;
		if (Frac > Precision) {
			while (true) {
				d = 1.0 / d;
				i = ((long) (d + Precision));
				d -= i;
				if (A > 0) {
					Num = (i * Num) + B;
				}
				Den = ((long) ((Num / Frac) + 0.5));
				Diff = Math.abs(((((double) (Num)) / Den) - Frac));
				if (Den > MaxDen) {
					if (A > 0) {
						Num = A;
						Den = ((long) ((Num / Frac) + 0.5));
						Diff = Math.abs(((((double) (Num)) / Den) - Frac));
					}else {
						Den = MaxDen;
						Num = 1;
						Diff = Math.abs(((((double) (Num)) / Den) - Frac));
						if (Diff > Frac) {
							Num = 0;
							Den = 1;
							Diff = Frac;
						}
					}
					break;
				}
				if ((Diff <= AllowedError) || (d < Precision)) {
					break;
				}
				Precision = AllowedError / Diff;
				B = A;
				A = Num;
			} 
		}
		if (Num == Den) {
			Whole++;
			Num = 0;
			Den = 0;
		}else
			if (Den == 0) {
				Num = 0;
			}

		if (sign < 0) {
			if (Whole == 0) {
				Num = -Num;
			}else {
				Whole = -Whole;
			}
		}
		return new StringBuffer().append(Whole).append(" ").append(Num).append("/").append(Den).toString();
	}

	private String formatUnit(double f, int units) {
		long Whole = ((long) (f));
		f -= Whole;
		long Num = Math.round((f * units));
		return new StringBuffer().append(Whole).append(" ").append(Num).append("/").append(units).toString();
	}

	public final String format(double val) {
		if ((mode) == (ONE_DIGIT)) {
			return format(val, 9);
		}else
			if ((mode) == (TWO_DIGIT)) {
				return format(val, 99);
			}else
				if ((mode) == (THREE_DIGIT)) {
					return format(val, 999);
				}else
					if ((mode) == (UNITS)) {
						return formatUnit(val, units);
					}



		throw new RuntimeException("Unexpected Case");
	}

	@Override
	public StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
		if (obj instanceof Number) {
			toAppendTo.append(format(((Number) (obj)).doubleValue()));
			return toAppendTo;
		}
		throw new IllegalArgumentException("Can only handle Numbers");
	}

	@Override
	public Object parseObject(String source, ParsePosition status) {
		return null;
	}

	@Override
	public Object parseObject(String source) throws ParseException {
		return null;
	}

	@Override
	public Object clone() {
		return null;
	}
}


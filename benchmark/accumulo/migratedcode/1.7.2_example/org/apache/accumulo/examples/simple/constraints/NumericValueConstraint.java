package org.apache.accumulo.examples.simple.constraints;


import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;


public class NumericValueConstraint implements Constraint {
	static final short NON_NUMERIC_VALUE = 1;

	static final String VIOLATION_MESSAGE = "Value is not numeric";

	private static final List<Short> VIOLATION_LIST = Collections.unmodifiableList(Arrays.asList(NumericValueConstraint.NON_NUMERIC_VALUE));

	private boolean isNumeric(byte[] bytes) {
		for (byte b : bytes) {
			boolean ok = (b >= '0') && (b <= '9');
			if (!ok)
				return false;

		}
		return true;
	}

	@Override
	public List<Short> check(Constraint.Environment env, Mutation mutation) {
		Collection<ColumnUpdate> updates = mutation.getUpdates();
		for (ColumnUpdate columnUpdate : updates) {
			if (!(isNumeric(columnUpdate.getValue())))
				return NumericValueConstraint.VIOLATION_LIST;

		}
		return null;
	}

	@Override
	public String getViolationDescription(short violationCode) {
		switch (violationCode) {
			case NumericValueConstraint.NON_NUMERIC_VALUE :
				return "Value is not numeric";
		}
		return null;
	}
}


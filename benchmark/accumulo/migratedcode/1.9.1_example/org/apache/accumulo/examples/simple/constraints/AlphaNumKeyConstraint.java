package org.apache.accumulo.examples.simple.constraints;


import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;


public class AlphaNumKeyConstraint implements Constraint {
	static final short NON_ALPHA_NUM_ROW = 1;

	static final short NON_ALPHA_NUM_COLF = 2;

	static final short NON_ALPHA_NUM_COLQ = 3;

	static final String ROW_VIOLATION_MESSAGE = "Row was not alpha numeric";

	static final String COLF_VIOLATION_MESSAGE = "Column family was not alpha numeric";

	static final String COLQ_VIOLATION_MESSAGE = "Column qualifier was not alpha numeric";

	private boolean isAlphaNum(byte[] bytes) {
		for (byte b : bytes) {
			boolean ok = (((b >= 'a') && (b <= 'z')) || ((b >= 'A') && (b <= 'Z'))) || ((b >= '0') && (b <= '9'));
			if (!ok)
				return false;

		}
		return true;
	}

	private Set<Short> addViolation(Set<Short> violations, short violation) {
		if (violations == null) {
			violations = new LinkedHashSet<>();
			violations.add(violation);
		}else
			if (!(violations.contains(violation))) {
				violations.add(violation);
			}

		return violations;
	}

	@Override
	public List<Short> check(Constraint.Environment env, Mutation mutation) {
		Set<Short> violations = null;
		if (!(isAlphaNum(mutation.getRow())))
			violations = addViolation(violations, AlphaNumKeyConstraint.NON_ALPHA_NUM_ROW);

		Collection<ColumnUpdate> updates = mutation.getUpdates();
		for (ColumnUpdate columnUpdate : updates) {
			if (!(isAlphaNum(columnUpdate.getColumnFamily())))
				violations = addViolation(violations, AlphaNumKeyConstraint.NON_ALPHA_NUM_COLF);

			if (!(isAlphaNum(columnUpdate.getColumnQualifier())))
				violations = addViolation(violations, AlphaNumKeyConstraint.NON_ALPHA_NUM_COLQ);

		}
		return null == violations ? null : new ArrayList<>(violations);
	}

	@Override
	public String getViolationDescription(short violationCode) {
		switch (violationCode) {
			case AlphaNumKeyConstraint.NON_ALPHA_NUM_ROW :
				return AlphaNumKeyConstraint.ROW_VIOLATION_MESSAGE;
			case AlphaNumKeyConstraint.NON_ALPHA_NUM_COLF :
				return AlphaNumKeyConstraint.COLF_VIOLATION_MESSAGE;
			case AlphaNumKeyConstraint.NON_ALPHA_NUM_COLQ :
				return AlphaNumKeyConstraint.COLQ_VIOLATION_MESSAGE;
		}
		return null;
	}
}


package org.apache.accumulo.examples.simple.constraints;


import java.util.Collections;
import java.util.List;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.Mutation;


public class MaxMutationSize implements Constraint {
	static final long MAX_SIZE = (Runtime.getRuntime().maxMemory()) >> 8;

	static final List<Short> empty = Collections.emptyList();

	static final List<Short> violations = Collections.singletonList(Short.valueOf(((short) (0))));

	@Override
	public String getViolationDescription(short violationCode) {
		return String.format("mutation exceeded maximum size of %d", MaxMutationSize.MAX_SIZE);
	}

	@Override
	public List<Short> check(Constraint.Environment env, Mutation mutation) {
		if ((mutation.estimatedMemoryUsed()) < (MaxMutationSize.MAX_SIZE))
			return MaxMutationSize.empty;

		return MaxMutationSize.violations;
	}
}


package org.apache.accumulo.examples.simple.constraints;


import com.google.common.collect.Iterables;
import java.util.List;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Assert;


public class NumericValueConstraintTest {
	private NumericValueConstraint nvc = new NumericValueConstraint();

	@org.junit.Test
	public void testCheck() {
		Mutation goodMutation = new Mutation(new Text("r"));
		goodMutation.put(new Text("cf"), new Text("cq"), new Value("1234".getBytes()));
		Assert.assertNull(nvc.check(null, goodMutation));
		Mutation badMutation = new Mutation(new Text("r"));
		badMutation.put(new Text("cf"), new Text("cq"), new Value("foo1234".getBytes()));
		badMutation.put(new Text("cf2"), new Text("cq2"), new Value("foo1234".getBytes()));
		Assert.assertEquals(NumericValueConstraint.NON_NUMERIC_VALUE, Iterables.getOnlyElement(nvc.check(null, badMutation)).shortValue());
	}

	@org.junit.Test
	public void testGetViolationDescription() {
		Assert.assertEquals(NumericValueConstraint.VIOLATION_MESSAGE, nvc.getViolationDescription(NumericValueConstraint.NON_NUMERIC_VALUE));
		Assert.assertNull(nvc.getViolationDescription(((short) (2))));
	}
}


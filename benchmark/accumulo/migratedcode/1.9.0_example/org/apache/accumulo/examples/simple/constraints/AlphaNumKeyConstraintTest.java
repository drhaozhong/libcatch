package org.apache.accumulo.examples.simple.constraints;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Assert;


public class AlphaNumKeyConstraintTest {
	private AlphaNumKeyConstraint ankc = new AlphaNumKeyConstraint();

	@org.junit.Test
	public void test() {
		Mutation goodMutation = new Mutation(new Text("Row1"));
		goodMutation.put(new Text("Colf2"), new Text("ColQ3"), new Value("value".getBytes()));
		Assert.assertNull(ankc.check(null, goodMutation));
		Mutation badMutation = new Mutation(new Text("Row#1"));
		badMutation.put(new Text("Colf$2"), new Text("Colq%3"), new Value("value".getBytes()));
		Assert.assertEquals(ImmutableList.of(AlphaNumKeyConstraint.NON_ALPHA_NUM_ROW, AlphaNumKeyConstraint.NON_ALPHA_NUM_COLF, AlphaNumKeyConstraint.NON_ALPHA_NUM_COLQ), ankc.check(null, badMutation));
	}

	@org.junit.Test
	public void testGetViolationDescription() {
		Assert.assertEquals(AlphaNumKeyConstraint.ROW_VIOLATION_MESSAGE, ankc.getViolationDescription(AlphaNumKeyConstraint.NON_ALPHA_NUM_ROW));
		Assert.assertEquals(AlphaNumKeyConstraint.COLF_VIOLATION_MESSAGE, ankc.getViolationDescription(AlphaNumKeyConstraint.NON_ALPHA_NUM_COLF));
		Assert.assertEquals(AlphaNumKeyConstraint.COLQ_VIOLATION_MESSAGE, ankc.getViolationDescription(AlphaNumKeyConstraint.NON_ALPHA_NUM_COLQ));
		Assert.assertNull(ankc.getViolationDescription(((short) (4))));
	}
}


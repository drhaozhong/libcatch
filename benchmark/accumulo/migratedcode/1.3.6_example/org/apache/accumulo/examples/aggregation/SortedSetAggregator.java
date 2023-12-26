package org.apache.accumulo.examples.aggregation;


import java.util.TreeSet;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.Aggregator;
import org.apache.accumulo.core.util.StringUtil;


public class SortedSetAggregator implements Aggregator {
	TreeSet<String> items = new TreeSet<String>();

	public Value aggregate() {
		return new Value(StringUtil.join(items, ",").getBytes());
	}

	public void collect(Value value) {
		String[] strings = value.toString().split(",");
		for (String s : strings)
			items.add(s);

	}

	public void reset() {
		items.clear();
	}
}


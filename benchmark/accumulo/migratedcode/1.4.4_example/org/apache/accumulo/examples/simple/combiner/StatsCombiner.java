package org.apache.accumulo.examples.simple.combiner;


import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;


public class StatsCombiner extends Combiner {
	public static final String RADIX_OPTION = "radix";

	private int radix = 10;

	@Override
	public Value reduce(Key key, Iterator<Value> iter) {
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		long sum = 0;
		long count = 0;
		while (iter.hasNext()) {
			String[] stats = iter.next().toString().split(",");
			if ((stats.length) == 1) {
				long val = Long.parseLong(stats[0], radix);
				min = Math.min(val, min);
				max = Math.max(val, max);
				sum += val;
				count += 1;
			}else {
				min = Math.min(Long.parseLong(stats[0], radix), min);
				max = Math.max(Long.parseLong(stats[1], radix), max);
				sum += Long.parseLong(stats[2], radix);
				count += Long.parseLong(stats[3], radix);
			}
		} 
		String ret = ((((((Long.toString(min, radix)) + ",") + (Long.toString(max, radix))) + ",") + (Long.toString(sum, radix))) + ",") + (Long.toString(count, radix));
		return new Value(ret.getBytes());
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		if (options.containsKey(StatsCombiner.RADIX_OPTION))
			radix = Integer.parseInt(options.get(StatsCombiner.RADIX_OPTION));
		else
			radix = 10;

	}

	@Override
	public OptionDescriber.IteratorOptions describeOptions() {
		OptionDescriber.IteratorOptions io = super.describeOptions();
		io.setName("statsCombiner");
		io.setDescription("Combiner that keeps track of min, max, sum, and count");
		io.addNamedOption(StatsCombiner.RADIX_OPTION, "radix/base of the numbers");
		return io;
	}

	@Override
	public boolean validateOptions(Map<String, String> options) {
		if (!(super.validateOptions(options)))
			return false;

		if ((options.containsKey(StatsCombiner.RADIX_OPTION)) && (!(options.get(StatsCombiner.RADIX_OPTION).matches("\\d+"))))
			return false;

		return true;
	}

	public static void setRadix(IteratorSetting iterConfig, int base) {
		iterConfig.addOption(StatsCombiner.RADIX_OPTION, (base + ""));
	}
}


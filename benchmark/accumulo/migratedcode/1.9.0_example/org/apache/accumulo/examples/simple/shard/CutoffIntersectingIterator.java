package org.apache.accumulo.examples.simple.shard;


import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.sample.RowColumnSampler;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.IntersectingIterator;


public class CutoffIntersectingIterator extends IntersectingIterator {
	private IntersectingIterator sampleII;

	private int sampleMax;

	private boolean hasTop;

	public static void setCutoff(IteratorSetting iterCfg, int cutoff) {
		Preconditions.checkArgument((cutoff >= 0));
		iterCfg.addOption("cutoff", (cutoff + ""));
	}

	@Override
	public boolean hasTop() {
		return (hasTop) && (super.hasTop());
	}

	@Override
	public void seek(Range range, Collection<ByteSequence> seekColumnFamilies, boolean inclusive) throws IOException {
		sampleII.seek(range, seekColumnFamilies, inclusive);
		int count = 0;
		while ((count <= (sampleMax)) && (sampleII.hasTop())) {
			sampleII.next();
			count++;
		} 
		if (count > (sampleMax)) {
			hasTop = false;
		}else {
			hasTop = true;
			super.seek(range, seekColumnFamilies, inclusive);
		}
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		IteratorEnvironment sampleEnv = env.cloneWithSamplingEnabled();
		setMax(sampleEnv, options);
		SortedKeyValueIterator<Key, Value> sampleDC = source.deepCopy(sampleEnv);
		sampleII = new IntersectingIterator();
		sampleII.init(sampleDC, options, env);
	}

	static void validateSamplerConfig(SamplerConfiguration sampleConfig) {
		Objects.requireNonNull(sampleConfig);
		Preconditions.checkArgument(sampleConfig.getSamplerClassName().equals(RowColumnSampler.class.getName()), ("Unexpected Sampler " + (sampleConfig.getSamplerClassName())));
		Preconditions.checkArgument(sampleConfig.getOptions().get("qualifier").equals("true"), "Expected sample on column qualifier");
		Preconditions.checkArgument(CutoffIntersectingIterator.isNullOrFalse(sampleConfig.getOptions(), "row", "family", "visibility"), "Expected sample on column qualifier only");
	}

	private void setMax(IteratorEnvironment sampleEnv, Map<String, String> options) {
		String cutoffValue = options.get("cutoff");
		SamplerConfiguration sampleConfig = sampleEnv.getSamplerConfiguration();
		Objects.requireNonNull(cutoffValue, "Expected cutoff option is missing");
		CutoffIntersectingIterator.validateSamplerConfig(sampleConfig);
		int modulus = Integer.parseInt(sampleConfig.getOptions().get("modulus"));
		sampleMax = Math.round(((Float.parseFloat(cutoffValue)) / modulus));
	}

	private static boolean isNullOrFalse(Map<String, String> options, String... keys) {
		for (String key : keys) {
			String val = options.get(key);
			if ((val != null) && (val.equals("true"))) {
				return false;
			}
		}
		return true;
	}
}


package org.apache.accumulo.examples.wikisearch.iterator;


import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.examples.wikisearch.protobuf.TermWeight;
import org.apache.accumulo.examples.wikisearch.protobuf.TermWeight.Info;

import static org.apache.accumulo.examples.wikisearch.protobuf.TermWeight.Info.newBuilder;
import static org.apache.accumulo.examples.wikisearch.protobuf.TermWeight.Info.parseFrom;


public class TextIndexCombiner extends TypedValueCombiner<TermWeight.Info> {
	public static final TypedValueCombiner.Encoder<TermWeight.Info> TERMWEIGHT_INFO_ENCODER = new TextIndexCombiner.TermWeightInfoEncoder();

	@Override
	public TermWeight.Info typedReduce(Key key, Iterator<TermWeight.Info> iter) {
		TermWeight.Info.Builder builder = newBuilder();
		List<Integer> offsets = new ArrayList<Integer>();
		float normalizedTermFrequency = 0.0F;
		while (iter.hasNext()) {
			TermWeight.Info info = iter.next();
			if (null == info)
				continue;

			for (int offset : info.getWordOffsetList()) {
				int pos = Collections.binarySearch(offsets, offset);
				if (pos < 0) {
					offsets.add((((-1) * pos) - 1), offset);
				}else {
					offsets.add(pos, offset);
				}
			}
			if ((info.getNormalizedTermFrequency()) > 0) {
				normalizedTermFrequency += info.getNormalizedTermFrequency();
			}
		} 
		for (int i = 0; i < (offsets.size()); ++i) {
			builder.addWordOffset(offsets.get(i));
		}
		builder.setNormalizedTermFrequency(normalizedTermFrequency);
		return builder.build();
	}

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		setEncoder(TextIndexCombiner.TERMWEIGHT_INFO_ENCODER);
	}

	public static class TermWeightInfoEncoder implements TypedValueCombiner.Encoder<TermWeight.Info> {
		@Override
		public byte[] encode(TermWeight.Info v) {
			return v.toByteArray();
		}

		@Override
		public TermWeight.Info decode(byte[] b) {
			if ((b.length) == 0)
				return null;

			try {
				return parseFrom(b);
			} catch (InvalidProtocolBufferException e) {
				throw new ValueFormatException("Value passed to aggregator was not of type TermWeight.Info");
			}
		}
	}
}


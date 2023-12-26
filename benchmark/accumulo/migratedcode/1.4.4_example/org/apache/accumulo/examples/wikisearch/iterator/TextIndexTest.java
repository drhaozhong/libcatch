package org.apache.accumulo.examples.wikisearch.iterator;


import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.examples.wikisearch.protobuf.TermWeight;

import static org.apache.accumulo.examples.wikisearch.protobuf.TermWeight.Info.newBuilder;
import static org.apache.accumulo.examples.wikisearch.protobuf.TermWeight.Info.parseFrom;


public class TextIndexTest {
	private TextIndexCombiner combiner;

	private List<Value> values;

	@org.junit.Before
	public void setup() throws Exception {
		combiner = new TextIndexCombiner();
		combiner.init(null, Collections.singletonMap("all", "true"), null);
		values = new ArrayList<Value>();
	}

	@org.junit.After
	public void cleanup() {
	}

	private TermWeight.Info.Builder createBuilder() {
		return newBuilder();
	}

	@org.junit.Test
	public void testSingleValue() throws InvalidProtocolBufferException {
		TermWeight.Info.Builder builder = createBuilder();
		builder.addWordOffset(1);
		builder.addWordOffset(5);
		builder.setNormalizedTermFrequency(0.1F);
		values.add(new Value(builder.build().toByteArray()));
		Value result = combiner.reduce(new Key(), values.iterator());
		TermWeight.Info info = parseFrom(result.get());
		Assert.assertTrue(((info.getNormalizedTermFrequency()) == 0.1F));
		List<Integer> offsets = info.getWordOffsetList();
		Assert.assertTrue(((offsets.size()) == 2));
		Assert.assertTrue(((offsets.get(0)) == 1));
		Assert.assertTrue(((offsets.get(1)) == 5));
	}

	@org.junit.Test
	public void testAggregateTwoValues() throws InvalidProtocolBufferException {
		TermWeight.Info.Builder builder = createBuilder();
		builder.addWordOffset(1);
		builder.addWordOffset(5);
		builder.setNormalizedTermFrequency(0.1F);
		values.add(new Value(builder.build().toByteArray()));
		builder = createBuilder();
		builder.addWordOffset(3);
		builder.setNormalizedTermFrequency(0.05F);
		values.add(new Value(builder.build().toByteArray()));
		Value result = combiner.reduce(new Key(), values.iterator());
		TermWeight.Info info = parseFrom(result.get());
		Assert.assertTrue(((info.getNormalizedTermFrequency()) == 0.15F));
		List<Integer> offsets = info.getWordOffsetList();
		Assert.assertTrue(((offsets.size()) == 3));
		Assert.assertTrue(((offsets.get(0)) == 1));
		Assert.assertTrue(((offsets.get(1)) == 3));
		Assert.assertTrue(((offsets.get(2)) == 5));
	}

	@org.junit.Test
	public void testAggregateManyValues() throws InvalidProtocolBufferException {
		TermWeight.Info.Builder builder = createBuilder();
		builder.addWordOffset(13);
		builder.addWordOffset(15);
		builder.addWordOffset(19);
		builder.setNormalizedTermFrequency(0.12F);
		values.add(new Value(builder.build().toByteArray()));
		builder = createBuilder();
		builder.addWordOffset(1);
		builder.addWordOffset(5);
		builder.setNormalizedTermFrequency(0.1F);
		values.add(new Value(builder.build().toByteArray()));
		builder = createBuilder();
		builder.addWordOffset(3);
		builder.setNormalizedTermFrequency(0.05F);
		values.add(new Value(builder.build().toByteArray()));
		Value result = combiner.reduce(new Key(), values.iterator());
		TermWeight.Info info = parseFrom(result.get());
		Assert.assertTrue(((info.getNormalizedTermFrequency()) == 0.27F));
		List<Integer> offsets = info.getWordOffsetList();
		Assert.assertTrue(((offsets.size()) == 6));
		Assert.assertTrue(((offsets.get(0)) == 1));
		Assert.assertTrue(((offsets.get(1)) == 3));
		Assert.assertTrue(((offsets.get(2)) == 5));
		Assert.assertTrue(((offsets.get(3)) == 13));
		Assert.assertTrue(((offsets.get(4)) == 15));
		Assert.assertTrue(((offsets.get(5)) == 19));
	}

	@org.junit.Test
	public void testEmptyValue() throws InvalidProtocolBufferException {
		TermWeight.Info.Builder builder = createBuilder();
		builder.addWordOffset(13);
		builder.addWordOffset(15);
		builder.addWordOffset(19);
		builder.setNormalizedTermFrequency(0.12F);
		values.add(new Value("".getBytes()));
		values.add(new Value(builder.build().toByteArray()));
		values.add(new Value("".getBytes()));
		builder = createBuilder();
		builder.addWordOffset(1);
		builder.addWordOffset(5);
		builder.setNormalizedTermFrequency(0.1F);
		values.add(new Value(builder.build().toByteArray()));
		values.add(new Value("".getBytes()));
		builder = createBuilder();
		builder.addWordOffset(3);
		builder.setNormalizedTermFrequency(0.05F);
		values.add(new Value(builder.build().toByteArray()));
		values.add(new Value("".getBytes()));
		Value result = combiner.reduce(new Key(), values.iterator());
		TermWeight.Info info = parseFrom(result.get());
		Assert.assertTrue(((info.getNormalizedTermFrequency()) == 0.27F));
		List<Integer> offsets = info.getWordOffsetList();
		Assert.assertTrue(((offsets.size()) == 6));
		Assert.assertTrue(((offsets.get(0)) == 1));
		Assert.assertTrue(((offsets.get(1)) == 3));
		Assert.assertTrue(((offsets.get(2)) == 5));
		Assert.assertTrue(((offsets.get(3)) == 13));
		Assert.assertTrue(((offsets.get(4)) == 15));
		Assert.assertTrue(((offsets.get(5)) == 19));
	}
}


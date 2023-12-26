package org.apache.accumulo.examples.wikisearch.iterator;


import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.AbstractCollection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid.List;

import static org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.newBuilder;
import static org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.parseFrom;


public class GlobalIndexUidCombiner extends TypedValueCombiner<Uid.List> {
	public static final TypedValueCombiner.Encoder<Uid.List> UID_LIST_ENCODER = new GlobalIndexUidCombiner.UidListEncoder();

	public static final int MAX = 20;

	@Override
	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		super.init(source, options, env);
		setEncoder(GlobalIndexUidCombiner.UID_LIST_ENCODER);
	}

	@Override
	public Uid.List typedReduce(Key key, Iterator<Uid.List> iter) {
		Uid.List.Builder builder = newBuilder();
		HashSet<String> uids = new HashSet<String>();
		boolean seenIgnore = false;
		long count = 0;
		while (iter.hasNext()) {
			Uid.List v = iter.next();
			if (null == v)
				continue;

			count = count + (v.getCOUNT());
			if (v.getIGNORE()) {
				seenIgnore = true;
			}
			uids.addAll(v.getUIDList());
		} 
		builder.setCOUNT(count);
		if (((uids.size()) > (GlobalIndexUidCombiner.MAX)) || seenIgnore) {
			builder.setIGNORE(true);
			builder.clearUID();
		}else {
			builder.setIGNORE(false);
			builder.addAllUID(uids);
		}
		return builder.build();
	}

	public static class UidListEncoder implements TypedValueCombiner.Encoder<Uid.List> {
		@Override
		public byte[] encode(Uid.List v) {
			return v.toByteArray();
		}

		@Override
		public Uid.List decode(byte[] b) {
			if ((b.length) == 0)
				return null;

			try {
				return parseFrom(b);
			} catch (InvalidProtocolBufferException e) {
				throw new ValueFormatException("Value passed to aggregator was not of type Uid.List");
			}
		}
	}
}


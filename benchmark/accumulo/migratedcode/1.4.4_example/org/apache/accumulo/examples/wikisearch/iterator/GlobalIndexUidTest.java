package org.apache.accumulo.examples.wikisearch.iterator;


import com.google.protobuf.AbstractMessageLite;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.examples.wikisearch.protobuf.Uid;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;

import static org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.newBuilder;
import static org.apache.accumulo.examples.wikisearch.protobuf.Uid.List.parseFrom;


public class GlobalIndexUidTest {
	private GlobalIndexUidCombiner combiner;

	private List<Value> values;

	@org.junit.Before
	public void setup() throws Exception {
		combiner = new GlobalIndexUidCombiner();
		combiner.init(null, Collections.singletonMap("all", "true"), null);
		values = new ArrayList<Value>();
	}

	private Uid.List.Builder createNewUidList() {
		return newBuilder();
	}

	@org.junit.Test
	public void testSingleUid() {
		Uid.List.Builder b = createNewUidList();
		b.setCOUNT(1);
		b.setIGNORE(false);
		b.addUID(UUID.randomUUID().toString());
		Uid.List uidList = b.build();
		Value val = new Value(uidList.toByteArray());
		values.add(val);
		Value result = combiner.reduce(new Key(), values.iterator());
		Assert.assertTrue(((val.compareTo(result.get())) == 0));
	}

	@org.junit.Test
	public void testLessThanMax() throws Exception {
		List<String> savedUUIDs = new ArrayList<String>();
		for (int i = 0; i < ((GlobalIndexUidCombiner.MAX) - 1); i++) {
			Uid.List.Builder b = createNewUidList();
			b.setIGNORE(false);
			String uuid = UUID.randomUUID().toString();
			savedUUIDs.add(uuid);
			b.setCOUNT(i);
			b.addUID(uuid);
			Uid.List uidList = b.build();
			Value val = new Value(uidList.toByteArray());
			values.add(val);
		}
		Value result = combiner.reduce(new Key(), values.iterator());
		Uid.List resultList = parseFrom(result.get());
		Assert.assertTrue(((resultList.getIGNORE()) == false));
		Assert.assertTrue(((resultList.getUIDCount()) == ((GlobalIndexUidCombiner.MAX) - 1)));
		List<String> resultListUUIDs = resultList.getUIDList();
		for (String s : savedUUIDs)
			Assert.assertTrue(resultListUUIDs.contains(s));

	}

	@org.junit.Test
	public void testEqualsMax() throws Exception {
		List<String> savedUUIDs = new ArrayList<String>();
		for (int i = 0; i < (GlobalIndexUidCombiner.MAX); i++) {
			Uid.List.Builder b = createNewUidList();
			b.setIGNORE(false);
			String uuid = UUID.randomUUID().toString();
			savedUUIDs.add(uuid);
			b.setCOUNT(i);
			b.addUID(uuid);
			Uid.List uidList = b.build();
			Value val = new Value(uidList.toByteArray());
			values.add(val);
		}
		Value result = combiner.reduce(new Key(), values.iterator());
		Uid.List resultList = parseFrom(result.get());
		Assert.assertTrue(((resultList.getIGNORE()) == false));
		Assert.assertTrue(((resultList.getUIDCount()) == (GlobalIndexUidCombiner.MAX)));
		List<String> resultListUUIDs = resultList.getUIDList();
		for (String s : savedUUIDs)
			Assert.assertTrue(resultListUUIDs.contains(s));

	}

	@org.junit.Test
	public void testMoreThanMax() throws Exception {
		List<String> savedUUIDs = new ArrayList<String>();
		for (int i = 0; i < ((GlobalIndexUidCombiner.MAX) + 10); i++) {
			Uid.List.Builder b = createNewUidList();
			b.setIGNORE(false);
			String uuid = UUID.randomUUID().toString();
			savedUUIDs.add(uuid);
			b.setCOUNT(1);
			b.addUID(uuid);
			Uid.List uidList = b.build();
			Value val = new Value(uidList.toByteArray());
			values.add(val);
		}
		Value result = combiner.reduce(new Key(), values.iterator());
		Uid.List resultList = parseFrom(result.get());
		Assert.assertTrue(((resultList.getIGNORE()) == true));
		Assert.assertTrue(((resultList.getUIDCount()) == 0));
		Assert.assertTrue(((resultList.getCOUNT()) == ((GlobalIndexUidCombiner.MAX) + 10)));
	}

	@org.junit.Test
	public void testSeenIgnore() throws Exception {
		Uid.List.Builder b = createNewUidList();
		b.setIGNORE(true);
		b.setCOUNT(0);
		Uid.List uidList = b.build();
		Value val = new Value(uidList.toByteArray());
		values.add(val);
		b = createNewUidList();
		b.setIGNORE(false);
		b.setCOUNT(1);
		b.addUID(UUID.randomUUID().toString());
		uidList = b.build();
		val = new Value(uidList.toByteArray());
		values.add(val);
		Value result = combiner.reduce(new Key(), values.iterator());
		Uid.List resultList = parseFrom(result.get());
		Assert.assertTrue(((resultList.getIGNORE()) == true));
		Assert.assertTrue(((resultList.getUIDCount()) == 0));
		Assert.assertTrue(((resultList.getCOUNT()) == 1));
	}

	@org.junit.Test
	public void testInvalidValueType() throws Exception {
		Combiner comb = new GlobalIndexUidCombiner();
		IteratorSetting setting = new IteratorSetting(1, GlobalIndexUidCombiner.class);
		GlobalIndexUidCombiner.setCombineAllColumns(setting, true);
		GlobalIndexUidCombiner.setLossyness(setting, true);
		comb.init(null, setting.getOptions(), null);
		Logger.getLogger(GlobalIndexUidCombiner.class).setLevel(Level.OFF);
		Value val = new Value(UUID.randomUUID().toString().getBytes());
		values.add(val);
		Value result = comb.reduce(new Key(), values.iterator());
		Uid.List resultList = parseFrom(result.get());
		Assert.assertTrue(((resultList.getIGNORE()) == false));
		Assert.assertTrue(((resultList.getUIDCount()) == 0));
		Assert.assertTrue(((resultList.getCOUNT()) == 0));
	}

	@org.junit.Test
	public void testCount() throws Exception {
		UUID uuid = UUID.randomUUID();
		for (int i = 0; i < 5; i++) {
			Uid.List.Builder b = createNewUidList();
			b.setCOUNT(1);
			b.setIGNORE(false);
			b.addUID(uuid.toString());
			Uid.List uidList = b.build();
			Value val = new Value(uidList.toByteArray());
			values.add(val);
		}
		Value result = combiner.reduce(new Key(), values.iterator());
		Uid.List resultList = parseFrom(result.get());
		Assert.assertTrue(((resultList.getIGNORE()) == false));
		Assert.assertTrue(((resultList.getUIDCount()) == 1));
		Assert.assertTrue(((resultList.getCOUNT()) == 5));
	}
}




import com.google.common.util.concurrent.Uninterruptibles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CounterColumn;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WordCountSetup {
	private static final Logger logger = LoggerFactory.getLogger(WordCountSetup.class);

	public static final int TEST_COUNT = 6;

	public static void main(String[] args) throws Exception {
		Cassandra.Iface client = WordCountSetup.createConnection();
		WordCountSetup.setupKeyspace(client);
		client.set_keyspace(WordCount.KEYSPACE);
		Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap;
		Column c;
		c = new Column().setName(ByteBufferUtil.bytes("text1")).setValue(ByteBufferUtil.bytes("word1")).setTimestamp(System.currentTimeMillis());
		mutationMap = WordCountSetup.getMutationMap(ByteBufferUtil.bytes("key0"), WordCount.COLUMN_FAMILY, c);
		client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
		WordCountSetup.logger.info("added text1");
		c = new Column().setName(ByteBufferUtil.bytes("text2")).setValue(ByteBufferUtil.bytes("word1 word2")).setTimestamp(System.currentTimeMillis());
		mutationMap = WordCountSetup.getMutationMap(ByteBufferUtil.bytes("key0"), WordCount.COLUMN_FAMILY, c);
		client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
		WordCountSetup.logger.info("added text2");
		mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
		for (int i = 0; i < 1000; i++) {
			c = new Column().setName(ByteBufferUtil.bytes("text3")).setValue(ByteBufferUtil.bytes("word1")).setTimestamp(System.currentTimeMillis());
			WordCountSetup.addToMutationMap(mutationMap, ByteBufferUtil.bytes(("key" + i)), WordCount.COLUMN_FAMILY, c);
		}
		client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
		WordCountSetup.logger.info("added text3");
		mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
		for (int i = 0; i < 1000; i++) {
			Column c1 = new Column().setName(ByteBufferUtil.bytes("text4")).setValue(ByteBufferUtil.bytes("word1")).setTimestamp(System.currentTimeMillis());
			Column c2 = new Column().setName(ByteBufferUtil.bytes("int4")).setValue(ByteBufferUtil.bytes((i % 4))).setTimestamp(System.currentTimeMillis());
			ByteBuffer key = ByteBufferUtil.bytes(("key" + i));
			WordCountSetup.addToMutationMap(mutationMap, key, WordCount.COLUMN_FAMILY, c1);
			WordCountSetup.addToMutationMap(mutationMap, key, WordCount.COLUMN_FAMILY, c2);
		}
		client.batch_mutate(mutationMap, ConsistencyLevel.ONE);
		WordCountSetup.logger.info("added text4");
		final ByteBuffer key = ByteBufferUtil.bytes("key-if-verse1");
		final ColumnParent colParent = new ColumnParent(WordCountCounters.COUNTER_COLUMN_FAMILY);
		for (String sentence : WordCountSetup.sentenceData()) {
			client.add(key, colParent, new CounterColumn(ByteBufferUtil.bytes(sentence), ((long) (sentence.split("\\s").length))), ConsistencyLevel.ONE);
		}
		WordCountSetup.logger.info("added key-if-verse1");
		System.exit(0);
	}

	private static Map<ByteBuffer, Map<String, List<Mutation>>> getMutationMap(ByteBuffer key, String cf, Column c) {
		Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
		WordCountSetup.addToMutationMap(mutationMap, key, cf, c);
		return mutationMap;
	}

	private static void addToMutationMap(Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap, ByteBuffer key, String cf, Column c) {
		Map<String, List<Mutation>> cfMutation = mutationMap.get(key);
		if (cfMutation == null) {
			cfMutation = new HashMap<String, List<Mutation>>();
			mutationMap.put(key, cfMutation);
		}
		List<Mutation> mutationList = cfMutation.get(cf);
		if (mutationList == null) {
			mutationList = new ArrayList<Mutation>();
			cfMutation.put(cf, mutationList);
		}
		ColumnOrSuperColumn cc = new ColumnOrSuperColumn();
		Mutation m = new Mutation();
		cc.setColumn(c);
		m.setColumn_or_supercolumn(cc);
		mutationList.add(m);
	}

	private static void setupKeyspace(Cassandra.Iface client) throws InvalidRequestException, SchemaDisagreementException, TException {
		List<CfDef> cfDefList = new ArrayList<CfDef>();
		CfDef input = new CfDef(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
		input.setComparator_type("AsciiType");
		input.setColumn_metadata(Arrays.asList(new ColumnDef(ByteBufferUtil.bytes("text1"), "AsciiType"), new ColumnDef(ByteBufferUtil.bytes("text2"), "AsciiType"), new ColumnDef(ByteBufferUtil.bytes("text3"), "AsciiType"), new ColumnDef(ByteBufferUtil.bytes("text4"), "AsciiType"), new ColumnDef(ByteBufferUtil.bytes("int4"), "Int32Type").setIndex_name("int4idx").setIndex_type(IndexType.KEYS)));
		cfDefList.add(input);
		CfDef output = new CfDef(WordCount.KEYSPACE, WordCount.OUTPUT_COLUMN_FAMILY);
		output.setComparator_type("AsciiType");
		output.setDefault_validation_class("Int32Type");
		cfDefList.add(output);
		CfDef counterInput = new CfDef(WordCount.KEYSPACE, WordCountCounters.COUNTER_COLUMN_FAMILY);
		counterInput.setComparator_type("UTF8Type");
		counterInput.setDefault_validation_class("CounterColumnType");
		cfDefList.add(counterInput);
		KsDef ksDef = new KsDef(WordCount.KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
		ksDef.putToStrategy_options("replication_factor", "1");
		client.system_add_keyspace(ksDef);
		int magnitude = WordCountSetup.getNumberOfHosts(client);
		Uninterruptibles.sleepUninterruptibly(magnitude, TimeUnit.SECONDS);
	}

	private static int getNumberOfHosts(Cassandra.Iface client) throws InvalidRequestException, TimedOutException, UnavailableException, TException {
		client.set_keyspace("system");
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(new byte[0]);
		sliceRange.setFinish(new byte[0]);
		predicate.setSlice_range(sliceRange);
		KeyRange keyrRange = new KeyRange();
		keyrRange.setStart_key(new byte[0]);
		keyrRange.setEnd_key(new byte[0]);
		ColumnParent parent = new ColumnParent("peers");
		List<KeySlice> ls = client.get_range_slices(parent, predicate, keyrRange, ConsistencyLevel.ONE);
		return ls.size();
	}

	private static Cassandra.Iface createConnection() throws TTransportException {
		if (((System.getProperty("cassandra.host")) == null) || ((System.getProperty("cassandra.port")) == null)) {
			WordCountSetup.logger.warn("cassandra.host or cassandra.port is not defined, using default");
		}
		return WordCountSetup.createConnection(System.getProperty("cassandra.host", "localhost"), Integer.valueOf(System.getProperty("cassandra.port", "9160")));
	}

	private static Cassandra.Client createConnection(String host, Integer port) throws TTransportException {
		TSocket socket = new TSocket(host, port);
		TTransport trans = new TFramedTransport(socket);
		trans.open();
		TProtocol protocol = new TBinaryProtocol(trans);
		return new Cassandra.Client(protocol);
	}

	private static String[] sentenceData() {
		return new String[]{ "If you can keep your head when all about you", "Are losing theirs and blaming it on you", "If you can trust yourself when all men doubt you,", "But make allowance for their doubting too:", "If you can wait and not be tired by waiting,", "Or being lied about, don’t deal in lies,", "Or being hated, don’t give way to hating,", "And yet don’t look too good, nor talk too wise;" };
	}
}


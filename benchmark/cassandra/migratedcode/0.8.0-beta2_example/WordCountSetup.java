

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TokenRange;
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

	public static final int TEST_COUNT = 4;

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
		System.exit(0);
	}

	private static Map<ByteBuffer, Map<String, List<Mutation>>> getMutationMap(ByteBuffer key, String cf, Column c) {
		Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
		WordCountSetup.addToMutationMap(mutationMap, key, cf, c);
		return mutationMap;
	}

	private static void addToMutationMap(Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap, ByteBuffer key, String cf, Column c) {
		Map<String, List<Mutation>> cfMutation = new HashMap<String, List<Mutation>>();
		List<Mutation> mList = new ArrayList<Mutation>();
		ColumnOrSuperColumn cc = new ColumnOrSuperColumn();
		Mutation m = new Mutation();
		cc.setColumn(c);
		m.setColumn_or_supercolumn(cc);
		mList.add(m);
		cfMutation.put(cf, mList);
		mutationMap.put(key, cfMutation);
	}

	private static void setupKeyspace(Cassandra.Iface client) throws InvalidRequestException, SchemaDisagreementException, TException {
		List<CfDef> cfDefList = new ArrayList<CfDef>();
		CfDef input = new CfDef(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
		input.setComparator_type("AsciiType");
		input.setDefault_validation_class("AsciiType");
		cfDefList.add(input);
		CfDef output = new CfDef(WordCount.KEYSPACE, WordCount.OUTPUT_COLUMN_FAMILY);
		output.setComparator_type("AsciiType");
		output.setDefault_validation_class("AsciiType");
		cfDefList.add(output);
		KsDef ksDef = new KsDef(WordCount.KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", cfDefList);
		ksDef.putToStrategy_options("replication_factor", "1");
		client.system_add_keyspace(ksDef);
		int magnitude = client.describe_ring(WordCount.KEYSPACE).size();
		try {
			Thread.sleep((1000 * magnitude));
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static Cassandra.Iface createConnection() throws TTransportException {
		if (((System.getProperty("cassandra.host")) == null) || ((System.getProperty("cassandra.port")) == null)) {
			WordCountSetup.logger.warn("cassandra.host or cassandra.port is not defined, using default");
		}
		return WordCountSetup.createConnection(System.getProperty("cassandra.host", "localhost"), Integer.valueOf(System.getProperty("cassandra.port", "9160")), Boolean.valueOf(System.getProperty("cassandra.framed", "true")));
	}

	private static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws TTransportException {
		TSocket socket = new TSocket(host, port);
		TTransport trans = (framed) ? new TFramedTransport(socket) : socket;
		trans.open();
		TProtocol protocol = new TBinaryProtocol(trans);
		return new Cassandra.Client(protocol);
	}
}


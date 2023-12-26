

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.cassandra.db.AbstractColumnContainer;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.InvalidRequestException;
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


public class ClientOnlyExample {
	private static final Logger logger = LoggerFactory.getLogger(ClientOnlyExample.class);

	private static final String KEYSPACE = "Keyspace1";

	private static final String COLUMN_FAMILY = "Standard1";

	private static void startClient() throws Exception {
		StorageService.instance.initClient();
		try {
			Thread.sleep(10000L);
		} catch (Exception ex) {
			throw new AssertionError(ex);
		}
	}

	private static void testWriting() throws Exception {
		for (int i = 0; i < 100; i++) {
			RowMutation change = new RowMutation(ClientOnlyExample.KEYSPACE, ByteBufferUtil.bytes(("key" + i)));
			ColumnPath cp = new ColumnPath(ClientOnlyExample.COLUMN_FAMILY).setColumn("colb".getBytes());
			change.add(new QueryPath(cp), ByteBufferUtil.bytes(("value" + i)), 0);
			System.out.println(("wrote key" + i));
		}
		System.out.println("Done writing.");
	}

	private static void testReading() throws Exception {
		Collection<ByteBuffer> cols = new ArrayList<ByteBuffer>() {
			{
				add(ByteBufferUtil.bytes("colb"));
			}
		};
		for (int i = 0; i < 100; i++) {
			List<ReadCommand> commands = new ArrayList<ReadCommand>();
			SliceByNamesReadCommand readCommand = new SliceByNamesReadCommand(ClientOnlyExample.KEYSPACE, ByteBufferUtil.bytes(("key" + i)), new QueryPath(ClientOnlyExample.COLUMN_FAMILY, null, null), cols);
			readCommand.setDigestQuery(false);
			commands.add(readCommand);
			List<Row> rows = StorageProxy.read(commands, ConsistencyLevel.ALL);
			assert (rows.size()) == 1;
			Row row = rows.get(0);
			ColumnFamily cf = row.cf;
			if (cf != null) {
				for (IColumn col : cf.getSortedColumns()) {
					System.out.println((((ByteBufferUtil.string(col.name())) + ", ") + (ByteBufferUtil.string(col.value()))));
				}
			}else
				System.err.println("This output indicates that nothing was read.");

		}
	}

	public static void main(String[] args) throws Exception {
		ClientOnlyExample.startClient();
		ClientOnlyExample.setupKeyspace(ClientOnlyExample.createConnection());
		ClientOnlyExample.testWriting();
		ClientOnlyExample.logger.info("Writing is done. Sleeping, then will try to read.");
		try {
			Thread.currentThread().sleep(10000);
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
		ClientOnlyExample.testReading();
		StorageService.instance.stopClient();
		System.exit(0);
	}

	private static void setupKeyspace(Cassandra.Iface client) throws InvalidRequestException, TException {
		List<CfDef> cfDefList = new ArrayList<CfDef>();
		CfDef columnFamily = new CfDef(ClientOnlyExample.KEYSPACE, ClientOnlyExample.COLUMN_FAMILY);
		cfDefList.add(columnFamily);
		try {
			int magnitude = client.describe_ring(ClientOnlyExample.KEYSPACE).size();
			try {
				Thread.sleep((1000 * magnitude));
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		} catch (InvalidRequestException probablyExists) {
			ClientOnlyExample.logger.warn(("Problem creating keyspace: " + (probablyExists.getMessage())));
		}
	}

	private static Cassandra.Iface createConnection() throws TTransportException {
		if (((System.getProperty("cassandra.host")) == null) || ((System.getProperty("cassandra.port")) == null)) {
			ClientOnlyExample.logger.warn("cassandra.host or cassandra.port is not defined, using default");
		}
		return ClientOnlyExample.createConnection(System.getProperty("cassandra.host", "localhost"), Integer.valueOf(System.getProperty("cassandra.port", "9160")), Boolean.valueOf(System.getProperty("cassandra.framed", "true")));
	}

	private static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws TTransportException {
		TSocket socket = new TSocket(host, port);
		TTransport trans = (framed) ? new TFramedTransport(socket) : socket;
		trans.open();
		TProtocol protocol = new TBinaryProtocol(trans);
		return new Cassandra.Client(protocol);
	}
}


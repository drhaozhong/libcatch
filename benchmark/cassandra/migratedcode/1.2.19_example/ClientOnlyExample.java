

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClientOnlyExample {
	private static final Logger logger = LoggerFactory.getLogger(ClientOnlyExample.class);

	private static final String KEYSPACE = "keyspace1";

	private static final String COLUMN_FAMILY = "standard1";

	private static void startClient() throws Exception {
		StorageService.instance.initClient();
	}

	private static void testWriting() throws Exception {
		ClientState state = new ClientState(false);
		state.setKeyspace(ClientOnlyExample.KEYSPACE);
		for (int i = 0; i < 100; i++) {
			QueryProcessor.process(new StringBuilder().append("INSERT INTO ").append(ClientOnlyExample.COLUMN_FAMILY).append(" (id, name, value) VALUES ( 'key").append(i).append("', 'colb', 'value").append(i).append("' )").toString(), ConsistencyLevel.QUORUM, new QueryState(state));
			System.out.println(("wrote key" + i));
		}
		System.out.println("Done writing.");
	}

	private static void testReading() throws Exception {
		ClientState state = new ClientState(false);
		state.setKeyspace(ClientOnlyExample.KEYSPACE);
		for (int i = 0; i < 100; i++) {
			List<List<ByteBuffer>> rows = ((ResultMessage.Rows) (QueryProcessor.process(new StringBuilder().append("SELECT id, name, value FROM ").append(ClientOnlyExample.COLUMN_FAMILY).append(" WHERE id = 'key").append(i).append("'").toString(), ConsistencyLevel.QUORUM, new QueryState(state)))).result.rows;
			assert (rows.size()) == 1;
			List<ByteBuffer> r = rows.get(0);
			assert (r.size()) == 3;
			System.out.println(new StringBuilder().append("ID: ").append(AsciiType.instance.compose(r.get(0))).append(", Name: ").append(AsciiType.instance.compose(r.get(1))).append(", Value: ").append(AsciiType.instance.compose(r.get(2))).toString());
		}
	}

	public static void main(String[] args) throws Exception {
		ClientOnlyExample.startClient();
		ClientOnlyExample.setupKeyspace();
		ClientOnlyExample.testWriting();
		ClientOnlyExample.logger.info("Writing is done. Sleeping, then will try to read.");
		try {
			Thread.currentThread().sleep(1000);
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
		ClientOnlyExample.testReading();
		StorageService.instance.stopClient();
		System.exit(0);
	}

	private static void setupKeyspace() throws InterruptedException, RequestExecutionException, RequestValidationException {
		if (QueryProcessor.process(new StringBuilder().append("SELECT * FROM system.schema_keyspaces WHERE keyspace_name='").append(ClientOnlyExample.KEYSPACE).append("'").toString(), ConsistencyLevel.QUORUM).isEmpty()) {
			QueryProcessor.process(new StringBuilder().append("CREATE KEYSPACE ").append(ClientOnlyExample.KEYSPACE).append(" WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }").toString(), ConsistencyLevel.QUORUM);
			Thread.sleep(1000);
		}
		if (QueryProcessor.process(new StringBuilder().append("SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='").append(ClientOnlyExample.KEYSPACE).append("' AND columnfamily_name='").append(ClientOnlyExample.COLUMN_FAMILY).append("'").toString(), ConsistencyLevel.QUORUM).isEmpty()) {
			ClientState state = new ClientState();
			state.setKeyspace(ClientOnlyExample.KEYSPACE);
			QueryProcessor.process(new StringBuilder().append("CREATE TABLE ").append(ClientOnlyExample.COLUMN_FAMILY).append(" ( id ascii PRIMARY KEY, name ascii, value ascii )").toString(), ConsistencyLevel.QUORUM, new QueryState(state));
			Thread.sleep(1000);
		}
	}
}


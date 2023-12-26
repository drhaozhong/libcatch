

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.StorageService;
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
		for (int i = 0; i < 100; i++) {
			QueryProcessor.process(String.format("INSERT INTO %s.%s (id, name, value) VALUES ( 'key%d', 'colb', 'value%d')", ClientOnlyExample.KEYSPACE, ClientOnlyExample.COLUMN_FAMILY, i, i), ConsistencyLevel.QUORUM);
			System.out.println(("wrote key" + i));
		}
		System.out.println("Done writing.");
	}

	private static void testReading() throws Exception {
		for (int i = 0; i < 100; i++) {
			String query = String.format("SELECT id, name, value FROM %s.%s WHERE id = 'key%d'", ClientOnlyExample.KEYSPACE, ClientOnlyExample.COLUMN_FAMILY, i);
			UntypedResultSet.Row row = QueryProcessor.process(query, ConsistencyLevel.QUORUM).one();
			System.out.println(String.format("ID: %s, Name: %s, Value: %s", row.getString("id"), row.getString("name"), row.getString("value")));
		}
	}

	public static void main(String[] args) throws Exception {
		ClientOnlyExample.startClient();
		ClientOnlyExample.setupKeyspace();
		ClientOnlyExample.testWriting();
		ClientOnlyExample.logger.info("Writing is done. Sleeping, then will try to read.");
		Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
		ClientOnlyExample.testReading();
		StorageService.instance.stopClient();
		System.exit(0);
	}

	private static void setupKeyspace() throws InterruptedException, RequestExecutionException, RequestValidationException {
		QueryProcessor.process((("CREATE KEYSPACE IF NOT EXISTS " + (ClientOnlyExample.KEYSPACE)) + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"), ConsistencyLevel.ANY);
		QueryProcessor.process((((("CREATE TABLE IF NOT EXISTS " + (ClientOnlyExample.KEYSPACE)) + ".") + (ClientOnlyExample.COLUMN_FAMILY)) + " (id ascii PRIMARY KEY, name ascii, value ascii )"), ConsistencyLevel.ANY);
		TimeUnit.MILLISECONDS.sleep(1000);
	}
}


package org.apache.accumulo.examples.simple.filedata;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;


public class FileDataQuery {
	private Connector conn = null;

	List<Map.Entry<Key, Value>> lastRefs;

	private ChunkInputStream cis;

	Scanner scanner;

	public FileDataQuery(String instanceName, String zooKeepers, String user, AuthenticationToken token, String tableName, Authorizations auths) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		ZooKeeperInstance instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zooKeepers));
		conn = instance.getConnector(user, token);
		lastRefs = new ArrayList<>();
		cis = new ChunkInputStream();
		scanner = conn.createScanner(tableName, auths);
	}

	public List<Map.Entry<Key, Value>> getLastRefs() {
		return lastRefs;
	}

	public ChunkInputStream getData(String hash) throws IOException {
		scanner.setRange(new Range(hash));
		scanner.setBatchSize(1);
		lastRefs.clear();
		PeekingIterator<Map.Entry<Key, Value>> pi = new PeekingIterator<>(scanner.iterator());
		if (pi.hasNext()) {
			while (!(pi.peek().getKey().getColumnFamily().equals(FileDataIngest.CHUNK_CF))) {
				lastRefs.add(pi.peek());
				pi.next();
			} 
		}
		cis.clear();
		cis.setSource(pi);
		return cis;
	}

	public String getSomeData(String hash, int numBytes) throws IOException {
		ChunkInputStream is = getData(hash);
		byte[] buf = new byte[numBytes];
		if ((is.read(buf)) >= 0) {
			return new String(buf);
		}else {
			return "";
		}
	}
}


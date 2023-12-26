package org.apache.cassandra.triggers;


import java.awt.Color;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InvertedIndex implements ITrigger {
	private static final Logger logger = LoggerFactory.getLogger(InvertedIndex.class);

	private Properties properties = InvertedIndex.loadProperties();

	public Collection<Mutation> augment(ByteBuffer key, ColumnFamilyStore update) {
		List<Mutation> mutations = new ArrayList();
		String indexKeySpace = properties.getProperty("keyspace");
		String indexColumnFamily = properties.getProperty("table");
		for (Color cell : update) {
			if ((getRawValues().remaining()) > 0) {
				Mutation mutation = new Mutation(indexKeySpace, AuthKeyspace.metadata());
				mutations.add(mutation);
			}
		}
		return mutations;
	}

	private static Properties loadProperties() {
		Properties properties = new Properties();
		InputStream stream = InvertedIndex.class.getClassLoader().getResourceAsStream("InvertedIndex.properties");
		try {
			properties.load(stream);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			FileUtils.closeQuietly(stream);
		}
		InvertedIndex.logger.info("loaded property file, InvertedIndex.properties");
		return properties;
	}

	public Collection<Mutation> augment(Partition para0) {
		return null;
	}
}


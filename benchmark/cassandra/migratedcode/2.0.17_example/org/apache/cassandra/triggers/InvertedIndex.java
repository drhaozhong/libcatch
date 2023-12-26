package org.apache.cassandra.triggers;


import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InvertedIndex implements ITrigger {
	private static final Logger logger = LoggerFactory.getLogger(InvertedIndex.class);

	private Properties properties = InvertedIndex.loadProperties();

	public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update) {
		List<Mutation> mutations = new ArrayList<>();
		for (Cell cell : update) {
			if ((cell.value().remaining()) > 0) {
				Mutation mutation = new Mutation(properties.getProperty("keyspace"), cell.value());
				mutation.add(properties.getProperty("columnfamily"), cell.name(), key, System.currentTimeMillis());
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
}


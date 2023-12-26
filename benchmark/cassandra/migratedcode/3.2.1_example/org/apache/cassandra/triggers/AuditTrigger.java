package org.apache.cassandra.triggers;


import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Row.SimpleBuilder;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;


public class AuditTrigger implements ITrigger {
	private Properties properties = AuditTrigger.loadProperties();

	public Collection<Mutation> augment(Partition update) {
		String auditKeyspace = properties.getProperty("keyspace");
		String auditTable = properties.getProperty("table");
		SimpleBuilder audit = new SimpleBuilder(Schema.instance.getCFMetaData(auditKeyspace, auditTable), FBUtilities.timestampMicros(), UUIDGen.getTimeUUID());
		audit.add("keyspace_name", update.metadata().ksName);
		audit.add("table_name", update.metadata().cfName);
		audit.add("primary_key", update.metadata().getKeyValidator().getString(update.partitionKey().getKey()));
		return Collections.singletonList(audit.build());
	}

	private static Properties loadProperties() {
		Properties properties = new Properties();
		InputStream stream = AuditTrigger.class.getClassLoader().getResourceAsStream("AuditTrigger.properties");
		try {
			properties.load(stream);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			FileUtils.closeQuietly(stream);
		}
		return properties;
	}
}


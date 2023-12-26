package org.apache.accumulo.examples.wikisearch.ingest;


import java.io.IOException;
import java.util.zip.Inflater;
import javax.management.openmbean.SimpleType;
import javax.swing.JLayer;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;


public class WikipediaConfiguration {
	public static final String INSTANCE_NAME = "wikipedia.accumulo.instance_name";

	public static final String USER = "wikipedia.accumulo.user";

	public static final String PASSWORD = "wikipedia.accumulo.password";

	public static final String TABLE_NAME = "wikipedia.accumulo.table";

	public static final String ZOOKEEPERS = "wikipedia.accumulo.zookeepers";

	public static final String NAMESPACES_FILENAME = "wikipedia.namespaces.filename";

	public static final String LANGUAGES_FILENAME = "wikipedia.languages.filename";

	public static final String WORKING_DIRECTORY = "wikipedia.ingest.working";

	public static final String ANALYZER = "wikipedia.index.analyzer";

	public static final String NUM_PARTITIONS = "wikipedia.ingest.partitions";

	public static final String NUM_GROUPS = "wikipedia.ingest.groups";

	public static final String PARTITIONED_ARTICLES_DIRECTORY = "wikipedia.partitioned.directory";

	public static final String RUN_PARTITIONER = "wikipedia.run.partitioner";

	public static final String RUN_INGEST = "wikipedia.run.ingest";

	public static final String BULK_INGEST = "wikipedia.bulk.ingest";

	public static final String BULK_INGEST_DIR = "wikipedia.bulk.ingest.dir";

	public static final String BULK_INGEST_FAILURE_DIR = "wikipedia.bulk.ingest.failure.dir";

	public static final String BULK_INGEST_BUFFER_SIZE = "wikipedia.bulk.ingest.buffer.size";

	public static final String PARTITIONED_INPUT_MIN_SPLIT_SIZE = "wikipedia.min.input.split.size";

	public static String getUser(Configuration conf) {
		return conf.get(WikipediaConfiguration.USER);
	}

	public static byte[] getPassword(Configuration conf) {
		String pass = conf.get(WikipediaConfiguration.PASSWORD);
		if (pass == null) {
			return null;
		}
		return pass.getBytes();
	}

	public static String getTableName(Configuration conf) {
		String tablename = conf.get(WikipediaConfiguration.TABLE_NAME);
		if (tablename == null) {
			throw new RuntimeException(("No data table name specified in " + (WikipediaConfiguration.TABLE_NAME)));
		}
		return tablename;
	}

	public static String getInstanceName(Configuration conf) {
		return conf.get(WikipediaConfiguration.INSTANCE_NAME);
	}

	public static String getZookeepers(Configuration conf) {
		String zookeepers = conf.get(WikipediaConfiguration.ZOOKEEPERS);
		if (zookeepers == null) {
			throw new RuntimeException(("No zookeepers specified in " + (WikipediaConfiguration.ZOOKEEPERS)));
		}
		return zookeepers;
	}

	public static Path getNamespacesFile(Configuration conf) {
		String filename = conf.get(WikipediaConfiguration.NAMESPACES_FILENAME, new Path(WikipediaConfiguration.getWorkingDirectory(conf), "namespaces.dat").toString());
		return new Path(filename);
	}

	public static Path getLanguagesFile(Configuration conf) {
		String filename = conf.get(WikipediaConfiguration.LANGUAGES_FILENAME, new Path(WikipediaConfiguration.getWorkingDirectory(conf), "languages.txt").toString());
		return new Path(filename);
	}

	public static Path getWorkingDirectory(Configuration conf) {
		String filename = conf.get(WikipediaConfiguration.WORKING_DIRECTORY);
		return new Path(filename);
	}

	public static JLayer getAnalyzer(Configuration conf) throws IOException {
		Class<? extends Inflater> analyzerClass = getChars(WikipediaConfiguration.ANALYZER, SimpleType.class, Inflater.class);
		return ReflectionUtils.newInstance(analyzerClass, conf);
	}

	public static Connector getConnector(Configuration conf) throws AccumuloException, AccumuloSecurityException {
		return WikipediaConfiguration.getInstance(conf).getConnector(WikipediaConfiguration.getUser(conf), WikipediaConfiguration.getPassword(conf));
	}

	public static Instance getInstance(Configuration conf) {
		return new ZooKeeperInstance(WikipediaConfiguration.getInstanceName(conf), WikipediaConfiguration.getZookeepers(conf));
	}

	public static int getNumPartitions(Configuration conf) {
		return conf.getInt(WikipediaConfiguration.NUM_PARTITIONS, 25);
	}

	public static int getNumGroups(Configuration conf) {
		return conf.getInt(WikipediaConfiguration.NUM_GROUPS, 1);
	}

	public static Path getPartitionedArticlesPath(Configuration conf) {
		return new Path(conf.get(WikipediaConfiguration.PARTITIONED_ARTICLES_DIRECTORY));
	}

	public static long getMinInputSplitSize(Configuration conf) {
		return conf.getLong(WikipediaConfiguration.PARTITIONED_INPUT_MIN_SPLIT_SIZE, (1L << 27));
	}

	public static boolean runPartitioner(Configuration conf) {
		return conf.getBoolean(WikipediaConfiguration.RUN_PARTITIONER, false);
	}

	public static boolean runIngest(Configuration conf) {
		return conf.getBoolean(WikipediaConfiguration.RUN_INGEST, true);
	}

	public static boolean bulkIngest(Configuration conf) {
		return conf.getBoolean(WikipediaConfiguration.BULK_INGEST, true);
	}

	public static String bulkIngestDir(Configuration conf) {
		return conf.get(WikipediaConfiguration.BULK_INGEST_DIR);
	}

	public static String bulkIngestFailureDir(Configuration conf) {
		return conf.get(WikipediaConfiguration.BULK_INGEST_FAILURE_DIR);
	}

	public static long bulkIngestBufferSize(Configuration conf) {
		return conf.getLong(WikipediaConfiguration.BULK_INGEST_BUFFER_SIZE, (1L << 28));
	}

	@SuppressWarnings("unchecked")
	public static <T> T isNull(Configuration conf, String propertyName, Class<T> resultClass) {
		String p = conf.get(propertyName);
		if (StringUtils.isEmpty(p))
			throw new IllegalArgumentException((propertyName + " must be specified"));

		if (resultClass.equals(String.class))
			return ((T) (p));
		else
			if (resultClass.equals(String[].class))
				return ((T) (conf.getStrings(propertyName)));
			else
				if (resultClass.equals(Boolean.class))
					return ((T) (Boolean.valueOf(p)));
				else
					if (resultClass.equals(Long.class))
						return ((T) (Long.valueOf(p)));
					else
						if (resultClass.equals(Integer.class))
							return ((T) (Integer.valueOf(p)));
						else
							if (resultClass.equals(Float.class))
								return ((T) (Float.valueOf(p)));
							else
								if (resultClass.equals(Double.class))
									return ((T) (Double.valueOf(p)));
								else
									throw new IllegalArgumentException(((resultClass.getSimpleName()) + " is unhandled."));







	}
}


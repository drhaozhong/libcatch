package org.apache.accumulo.examples.simple.dirlist;


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.simple.filedata.ChunkCombiner;
import org.apache.accumulo.examples.simple.filedata.FileDataIngest;
import org.apache.hadoop.io.Text;


public class Ingest {
	static final Value nullValue = new Value(new byte[0]);

	public static final String LENGTH_CQ = "length";

	public static final String HIDDEN_CQ = "hidden";

	public static final String EXEC_CQ = "exec";

	public static final String LASTMOD_CQ = "lastmod";

	public static final String HASH_CQ = "md5";

	public static final TypedValueCombiner.Encoder<Long> encoder = LongCombiner.FIXED_LEN_ENCODER;

	public static Mutation buildMutation(ColumnVisibility cv, String path, boolean isDir, boolean isHidden, boolean canExec, long length, long lastmod, String hash) {
		if (path.equals("/"))
			path = "";

		Mutation m = new Mutation(QueryUtil.getRow(path));
		Text colf = null;
		if (isDir)
			colf = QueryUtil.DIR_COLF;
		else
			colf = new Text(Ingest.encoder.encode(((Long.MAX_VALUE) - lastmod)));

		m.put(colf, new Text(Ingest.LENGTH_CQ), cv, new Value(Long.toString(length).getBytes()));
		m.put(colf, new Text(Ingest.HIDDEN_CQ), cv, new Value(Boolean.toString(isHidden).getBytes()));
		m.put(colf, new Text(Ingest.EXEC_CQ), cv, new Value(Boolean.toString(canExec).getBytes()));
		m.put(colf, new Text(Ingest.LASTMOD_CQ), cv, new Value(Long.toString(lastmod).getBytes()));
		if ((hash != null) && ((hash.length()) > 0))
			m.put(colf, new Text(Ingest.HASH_CQ), cv, new Value(hash.getBytes()));

		return m;
	}

	private static void ingest(File src, ColumnVisibility cv, BatchWriter dirBW, BatchWriter indexBW, FileDataIngest fdi, BatchWriter data) throws Exception {
		String path = null;
		try {
			path = src.getCanonicalPath();
		} catch (IOException e) {
			path = src.getAbsolutePath();
		}
		System.out.println(path);
		String hash = null;
		if (!(src.isDirectory())) {
			try {
				hash = fdi.insertFileData(path, data);
			} catch (Exception e) {
				return;
			}
		}
		dirBW.addMutation(Ingest.buildMutation(cv, path, src.isDirectory(), src.isHidden(), src.canExecute(), src.length(), src.lastModified(), hash));
		Text row = QueryUtil.getForwardIndex(path);
		if (row != null) {
			Text p = new Text(QueryUtil.getRow(path));
			Mutation m = new Mutation(row);
			m.put(QueryUtil.INDEX_COLF, p, cv, Ingest.nullValue);
			indexBW.addMutation(m);
			row = QueryUtil.getReverseIndex(path);
			m = new Mutation(row);
			m.put(QueryUtil.INDEX_COLF, p, cv, Ingest.nullValue);
			indexBW.addMutation(m);
		}
	}

	private static void recurse(File src, ColumnVisibility cv, BatchWriter dirBW, BatchWriter indexBW, FileDataIngest fdi, BatchWriter data) throws Exception {
		Ingest.ingest(src, cv, dirBW, indexBW, fdi, data);
		if (src.isDirectory()) {
			File[] files = src.listFiles();
			if (files == null)
				return;

			for (File child : files) {
				Ingest.recurse(child, cv, dirBW, indexBW, fdi, data);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if ((args.length) < 10) {
			System.out.println((("usage: " + (Ingest.class.getSimpleName())) + " <instance> <zoo> <user> <pass> <dir table> <index table> <data table> <visibility> <data chunk size> <dir>{ <dir>}"));
			System.exit(1);
		}
		String instance = args[0];
		String zooKeepers = args[1];
		String user = args[2];
		String pass = args[3];
		String nameTable = args[4];
		String indexTable = args[5];
		String dataTable = args[6];
		ColumnVisibility colvis = new ColumnVisibility(args[7]);
		int chunkSize = Integer.parseInt(args[8]);
		Connector conn = new ZooKeeperInstance(instance, zooKeepers).getConnector(user, pass.getBytes());
		if (!(conn.tableOperations().exists(nameTable)))
			conn.tableOperations().create(nameTable);

		if (!(conn.tableOperations().exists(indexTable)))
			conn.tableOperations().create(indexTable);

		if (!(conn.tableOperations().exists(dataTable))) {
			conn.tableOperations().create(dataTable);
			conn.tableOperations().attachIterator(dataTable, new IteratorSetting(1, ChunkCombiner.class));
		}
		BatchWriter dirBW = conn.createBatchWriter(nameTable, 50000000, 300000L, 4);
		BatchWriter indexBW = conn.createBatchWriter(indexTable, 50000000, 300000L, 4);
		BatchWriter dataBW = conn.createBatchWriter(dataTable, 50000000, 300000L, 4);
		FileDataIngest fdi = new FileDataIngest(chunkSize, colvis);
		for (int i = 9; i < (args.length); i++) {
			Ingest.recurse(new File(args[i]), colvis, dirBW, indexBW, fdi, dataBW);
			String file = args[i];
			int slashIndex = -1;
			while ((slashIndex = file.lastIndexOf("/")) > 0) {
				file = file.substring(0, slashIndex);
				Ingest.ingest(new File(file), colvis, dirBW, indexBW, fdi, dataBW);
			} 
		}
		Ingest.ingest(new File("/"), colvis, dirBW, indexBW, fdi, dataBW);
		dirBW.close();
		indexBW.close();
		dataBW.close();
	}
}


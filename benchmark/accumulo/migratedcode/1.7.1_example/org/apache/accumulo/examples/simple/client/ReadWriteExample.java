package org.apache.accumulo.examples.simple.client;


import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.accumulo.core.cli.ClientOnDefaultTable;
import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.DurabilityImpl;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ByteArraySet;
import org.apache.hadoop.io.Text;


public class ReadWriteExample {
	private static final String DEFAULT_AUTHS = "LEVEL1,GROUP1";

	private static final String DEFAULT_TABLE_NAME = "test";

	private Connector conn;

	static class DurabilityConverter implements IStringConverter<Durability> {
		@Override
		public Durability convert(String value) {
			return DurabilityImpl.fromString(value);
		}
	}

	static class Opts extends ClientOnDefaultTable {
		@Parameter(names = { "-C", "--createtable" }, description = "create table before doing anything")
		boolean createtable = false;

		@Parameter(names = { "-D", "--deletetable" }, description = "delete table when finished")
		boolean deletetable = false;

		@Parameter(names = { "-c", "--create" }, description = "create entries before any deletes")
		boolean createEntries = false;

		@Parameter(names = { "-r", "--read" }, description = "read entries after any creates/deletes")
		boolean readEntries = false;

		@Parameter(names = { "-d", "--delete" }, description = "delete entries after any creates")
		boolean deleteEntries = false;

		@Parameter(names = { "--durability" }, description = "durability used for writes (none, log, flush or sync)", converter = ReadWriteExample.DurabilityConverter.class)
		Durability durability = Durability.DEFAULT;

		public Opts() {
			super(ReadWriteExample.DEFAULT_TABLE_NAME);
			auths = new Authorizations(ReadWriteExample.DEFAULT_AUTHS.split(","));
		}
	}

	private ReadWriteExample() {
	}

	private void execute(ReadWriteExample.Opts opts, ScannerOpts scanOpts) throws Exception {
		conn = opts.getConnector();
		Authorizations userAuthorizations = conn.securityOperations().getUserAuthorizations(opts.getPrincipal());
		ByteArraySet auths = new ByteArraySet(userAuthorizations.getAuthorizations());
		auths.addAll(opts.auths.getAuthorizations());
		if (!(auths.isEmpty()))
			conn.securityOperations().changeUserAuthorizations(opts.getPrincipal(), new Authorizations(auths));

		if (opts.createtable) {
			SortedSet<Text> partitionKeys = new TreeSet<Text>();
			for (int i = Byte.MIN_VALUE; i < (Byte.MAX_VALUE); i++)
				partitionKeys.add(new Text(new byte[]{ ((byte) (i)) }));

			conn.tableOperations().create(opts.getTableName());
			conn.tableOperations().addSplits(opts.getTableName(), partitionKeys);
		}
		createEntries(opts);
		if (opts.readEntries) {
			Scanner scanner = conn.createScanner(opts.getTableName(), opts.auths);
			scanner.setBatchSize(scanOpts.scanBatchSize);
			for (Map.Entry<Key, Value> entry : scanner)
				System.out.println((((entry.getKey().toString()) + " -> ") + (entry.getValue().toString())));

		}
		if (opts.deletetable)
			conn.tableOperations().delete(opts.getTableName());

	}

	private void createEntries(ReadWriteExample.Opts opts) throws Exception {
		if ((opts.createEntries) || (opts.deleteEntries)) {
			BatchWriterConfig cfg = new BatchWriterConfig();
			cfg.setDurability(opts.durability);
			BatchWriter writer = conn.createBatchWriter(opts.getTableName(), cfg);
			ColumnVisibility cv = new ColumnVisibility(opts.auths.toString().replace(',', '|'));
			Text cf = new Text("datatypes");
			Text cq = new Text("xml");
			byte[] row = new byte[]{ 'h', 'e', 'l', 'l', 'o', '\u0000' };
			byte[] value = new byte[]{ 'w', 'o', 'r', 'l', 'd', '\u0000' };
			for (int i = 0; i < 10; i++) {
				row[((row.length) - 1)] = ((byte) (i));
				Mutation m = new Mutation(new Text(row));
				if (opts.deleteEntries) {
					m.putDelete(cf, cq, cv);
				}
				if (opts.createEntries) {
					value[((value.length) - 1)] = ((byte) (i));
					m.put(cf, cq, cv, new Value(value));
				}
				writer.addMutation(m);
			}
			writer.close();
		}
	}

	public static void main(String[] args) throws Exception {
		ReadWriteExample rwe = new ReadWriteExample();
		ReadWriteExample.Opts opts = new ReadWriteExample.Opts();
		ScannerOpts scanOpts = new ScannerOpts();
		opts.parseArgs(ReadWriteExample.class.getName(), args, scanOpts);
		rwe.execute(opts, scanOpts);
	}
}


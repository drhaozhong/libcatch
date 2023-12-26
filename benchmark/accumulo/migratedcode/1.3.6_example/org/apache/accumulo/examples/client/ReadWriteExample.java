package org.apache.accumulo.examples.client;


import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class ReadWriteExample {
	private static final String DEFAULT_INSTANCE_NAME = "test";

	private static final String DEFAULT_ZOOKEEPERS = "localhost";

	private static final String DEFAULT_AUTHS = "LEVEL1,GROUP1";

	private static final String DEFAULT_TABLE_NAME = "test";

	private Option instanceOpt = new Option("i", "instance", true, "instance name");

	private Option zooKeepersOpt = new Option("z", "zooKeepers", true, "zoo keepers");

	private Option usernameOpt = new Option("u", "user", true, "user name");

	private Option passwordOpt = new Option("p", "password", true, "password");

	private Option scanAuthsOpt = new Option("s", "scanauths", true, "comma-separated scan authorizations");

	private Option tableNameOpt = new Option("t", "table", true, "table name");

	private Option createtableOpt = new Option("C", "createtable", false, "create table before doing anything");

	private Option deletetableOpt = new Option("D", "deletetable", false, "delete table when finished");

	private Option createEntriesOpt = new Option("e", "create", false, "create entries before any deletes");

	private Option deleteEntriesOpt = new Option("d", "delete", false, "delete entries after any creates");

	private Option readEntriesOpt = new Option("r", "read", false, "read entries after any creates/deletes");

	private Option debugOpt = new Option("dbg", "debug", false, "enable debugging");

	private Options opts;

	private CommandLine cl;

	private Connector conn;

	private ReadWriteExample() {
	}

	private void configure(String[] args) throws AccumuloException, AccumuloSecurityException, ParseException {
		usernameOpt.setRequired(true);
		passwordOpt.setRequired(true);
		opts = new Options();
		addOptions(instanceOpt, zooKeepersOpt, usernameOpt, passwordOpt, scanAuthsOpt, tableNameOpt, createtableOpt, deletetableOpt, createEntriesOpt, deleteEntriesOpt, readEntriesOpt, debugOpt);
		cl = new BasicParser().parse(opts, args);
		if ((cl.getArgs().length) != 0)
			throw new ParseException(("unrecognized options " + (cl.getArgList())));

		if (hasOpt(debugOpt))
			Logger.getLogger(Constants.CORE_PACKAGE_NAME).setLevel(Level.TRACE);

		Instance inst = new ZooKeeperInstance(getOpt(instanceOpt, ReadWriteExample.DEFAULT_INSTANCE_NAME), getOpt(zooKeepersOpt, ReadWriteExample.DEFAULT_ZOOKEEPERS));
		conn = inst.getConnector(getRequiredOpt(usernameOpt), getRequiredOpt(passwordOpt).getBytes());
	}

	private void addOptions(Option... addOpts) {
		for (Option opt : addOpts)
			opts.addOption(opt);

	}

	private boolean hasOpt(Option opt) {
		return cl.hasOption(opt.getOpt());
	}

	private String getRequiredOpt(Option opt) {
		return getOpt(opt, null);
	}

	private String getOpt(Option opt, String defaultValue) {
		return cl.getOptionValue(opt.getOpt(), defaultValue);
	}

	private void printHelp() {
		HelpFormatter hf = new HelpFormatter();
		instanceOpt.setArgName("name");
		zooKeepersOpt.setArgName("hosts");
		usernameOpt.setArgName("user");
		passwordOpt.setArgName("pass");
		scanAuthsOpt.setArgName("scanauths");
		tableNameOpt.setArgName("name");
		hf.printHelp("accumulo accumulo.examples.client.ReadWriteExample", opts, true);
	}

	private void execute() throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableExistsException, TableNotFoundException {
		if (hasOpt(createtableOpt)) {
			SortedSet<Text> partitionKeys = new TreeSet<Text>();
			for (int i = Byte.MIN_VALUE; i < (Byte.MAX_VALUE); i++)
				partitionKeys.add(new Text(new byte[]{ ((byte) (i)) }));

			conn.tableOperations().create(getOpt(tableNameOpt, ReadWriteExample.DEFAULT_TABLE_NAME));
			conn.tableOperations().addSplits(getOpt(tableNameOpt, ReadWriteExample.DEFAULT_TABLE_NAME), partitionKeys);
		}
		if (hasOpt(createEntriesOpt))
			createEntries(false);

		if (hasOpt(deleteEntriesOpt))
			createEntries(true);

		if (hasOpt(readEntriesOpt)) {
			Authorizations scanauths = new Authorizations(getOpt(scanAuthsOpt, ReadWriteExample.DEFAULT_AUTHS).split(","));
			Scanner scanner = conn.createScanner(getOpt(tableNameOpt, ReadWriteExample.DEFAULT_TABLE_NAME), scanauths);
			for (Map.Entry<Key, Value> entry : scanner)
				System.out.println((((entry.getKey().toString()) + " -> ") + (entry.getValue().toString())));

		}
		if (hasOpt(deletetableOpt))
			conn.tableOperations().delete(getOpt(tableNameOpt, ReadWriteExample.DEFAULT_TABLE_NAME));

	}

	private void createEntries(boolean delete) throws AccumuloException, MutationsRejectedException, TableNotFoundException {
		BatchWriter writer = conn.createBatchWriter(getOpt(tableNameOpt, ReadWriteExample.DEFAULT_TABLE_NAME), 10000, Long.MAX_VALUE, 1);
		ColumnVisibility cv = new ColumnVisibility(ReadWriteExample.DEFAULT_AUTHS.replace(',', '|'));
		Text cf = new Text("datatypes");
		Text cq = new Text("xml");
		byte[] row = new byte[]{ 'h', 'e', 'l', 'l', 'o', '\u0000' };
		byte[] value = new byte[]{ 'w', 'o', 'r', 'l', 'd', '\u0000' };
		for (int i = 0; i < 10; i++) {
			row[((row.length) - 1)] = ((byte) (i));
			Mutation m = new Mutation(new Text(row));
			if (delete) {
				m.putDelete(cf, cq, cv);
			}else {
				value[((value.length) - 1)] = ((byte) (i));
				m.put(cf, cq, cv, new Value(value));
			}
			writer.addMutation(m);
		}
		writer.close();
	}

	public static void main(String[] args) throws Exception {
		ReadWriteExample rwe = new ReadWriteExample();
		try {
			rwe.configure(args);
			rwe.execute();
		} catch (ParseException e) {
			System.err.println(e.getMessage());
			rwe.printHelp();
			System.exit(1);
		}
	}
}


package org.apache.accumulo.examples.simple.client;


import java.io.PrintStream;
import java.util.Map;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


public class RowOperations {
	private static final Logger log = Logger.getLogger(RowOperations.class);

	private static Connector connector;

	private static String table = "example";

	private static BatchWriter bw;

	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, MutationsRejectedException, TableExistsException, TableNotFoundException {
		if ((args.length) != 4) {
			RowOperations.log.error("Usage: <instance name> <zoo keepers> <username> <password>");
			return;
		}
		RowOperations.connector = new ZooKeeperInstance(args[0], args[1]).getConnector(args[2], args[3].getBytes());
		RowOperations.connector.tableOperations().create(RowOperations.table);
		Text row1 = new Text("row1");
		Text row2 = new Text("row2");
		Text row3 = new Text("row3");
		Mutation mut1 = new Mutation(row1);
		Mutation mut2 = new Mutation(row2);
		Mutation mut3 = new Mutation(row3);
		Text col1 = new Text("1");
		Text col2 = new Text("2");
		Text col3 = new Text("3");
		Text col4 = new Text("4");
		mut1.put(new Text("column"), col1, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut1.put(new Text("column"), col2, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut1.put(new Text("column"), col3, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut1.put(new Text("column"), col4, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut2.put(new Text("column"), col1, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut2.put(new Text("column"), col2, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut2.put(new Text("column"), col3, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut2.put(new Text("column"), col4, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut3.put(new Text("column"), col1, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut3.put(new Text("column"), col2, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut3.put(new Text("column"), col3, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		mut3.put(new Text("column"), col4, System.currentTimeMillis(), new Value("This is the value for this key".getBytes()));
		RowOperations.bw = RowOperations.connector.createBatchWriter(RowOperations.table, 100000L, 30L, 1);
		RowOperations.bw.addMutation(mut1);
		RowOperations.bw.addMutation(mut2);
		RowOperations.bw.addMutation(mut3);
		RowOperations.bw.flush();
		Scanner rowThree = RowOperations.getRow(new Text("row3"));
		Scanner rowTwo = RowOperations.getRow(new Text("row2"));
		Scanner rowOne = RowOperations.getRow(new Text("row1"));
		RowOperations.log.info("This is everything");
		RowOperations.printRow(rowOne);
		RowOperations.printRow(rowTwo);
		RowOperations.printRow(rowThree);
		System.out.flush();
		rowTwo = RowOperations.getRow(new Text("row2"));
		RowOperations.deleteRow(rowTwo);
		rowThree = RowOperations.getRow(new Text("row3"));
		rowTwo = RowOperations.getRow(new Text("row2"));
		rowOne = RowOperations.getRow(new Text("row1"));
		RowOperations.log.info("This is row1 and row3");
		RowOperations.printRow(rowOne);
		RowOperations.printRow(rowTwo);
		RowOperations.printRow(rowThree);
		System.out.flush();
		RowOperations.deleteRow(row1);
		rowThree = RowOperations.getRow(new Text("row3"));
		rowTwo = RowOperations.getRow(new Text("row2"));
		rowOne = RowOperations.getRow(new Text("row1"));
		RowOperations.log.info("This is just row3");
		RowOperations.printRow(rowOne);
		RowOperations.printRow(rowTwo);
		RowOperations.printRow(rowThree);
		System.out.flush();
		RowOperations.bw.close();
		RowOperations.connector.tableOperations().delete(RowOperations.table);
	}

	private static void deleteRow(Text row) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		RowOperations.deleteRow(RowOperations.getRow(row));
	}

	private static void deleteRow(Scanner scanner) throws MutationsRejectedException {
		Mutation deleter = null;
		for (Map.Entry<Key, Value> entry : scanner) {
			if (deleter == null)
				deleter = new Mutation(entry.getKey().getRow());

			deleter.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
		}
		RowOperations.bw.addMutation(deleter);
		RowOperations.bw.flush();
	}

	private static void printRow(Scanner scanner) {
		for (Map.Entry<Key, Value> entry : scanner)
			RowOperations.log.info(((("Key: " + (entry.getKey().toString())) + " Value: ") + (entry.getValue().toString())));

	}

	private static Scanner getRow(Text row) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		Scanner scanner = RowOperations.connector.createScanner(RowOperations.table, Constants.NO_AUTHS);
		scanner.setRange(new Range(row));
		return scanner;
	}
}


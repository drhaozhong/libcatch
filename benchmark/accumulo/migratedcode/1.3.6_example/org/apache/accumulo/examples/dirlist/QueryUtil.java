package org.apache.accumulo.examples.dirlist;


import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.aggregation.LongSummation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;


public class QueryUtil {
	private Connector conn = null;

	private String tableName;

	private Authorizations auths;

	public static final Text DIR_COLF = new Text("dir");

	public static final Text FORWARD_PREFIX = new Text("f");

	public static final Text REVERSE_PREFIX = new Text("r");

	public static final Text INDEX_COLF = new Text("i");

	public static final Text COUNTS_COLQ = new Text("counts");

	public QueryUtil(String instanceName, String zooKeepers, String user, String password, String tableName, Authorizations auths) throws AccumuloException, AccumuloSecurityException {
		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
		conn = instance.getConnector(user, password.getBytes());
		this.tableName = tableName;
		this.auths = auths;
	}

	public static int getDepth(String path) {
		int numSlashes = 0;
		int index = -1;
		while ((index = path.indexOf("/", (index + 1))) >= 0)
			numSlashes++;

		return numSlashes;
	}

	public static Text getRow(String path) {
		Text row = new Text(String.format("%03d", QueryUtil.getDepth(path)));
		row.append(path.getBytes(), 0, path.length());
		return row;
	}

	public static Text getForwardIndex(String path) {
		String part = path.substring(((path.lastIndexOf("/")) + 1));
		if ((part.length()) == 0)
			return null;

		Text row = new Text(QueryUtil.FORWARD_PREFIX);
		row.append(part.getBytes(), 0, part.length());
		return row;
	}

	public static Text getReverseIndex(String path) {
		String part = path.substring(((path.lastIndexOf("/")) + 1));
		if ((part.length()) == 0)
			return null;

		byte[] rev = new byte[part.length()];
		int i = (part.length()) - 1;
		for (byte b : part.getBytes())
			rev[(i--)] = b;

		Text row = new Text(QueryUtil.REVERSE_PREFIX);
		row.append(rev, 0, rev.length);
		return row;
	}

	public static String getType(Text colf) {
		if (colf.equals(QueryUtil.DIR_COLF))
			return (colf.toString()) + ":";

		try {
			return (Long.toString(LongSummation.bytesToLong(colf.getBytes()))) + ":";
		} catch (IOException e) {
			return (colf.toString()) + ":";
		}
	}

	public Map<String, String> getData(String path) throws TableNotFoundException {
		if (path.endsWith("/"))
			path = path.substring(0, ((path.length()) - 1));

		Scanner scanner = conn.createScanner(tableName, auths);
		scanner.setRange(new Range(QueryUtil.getRow(path)));
		Map<String, String> data = new TreeMap<String, String>();
		for (Map.Entry<Key, Value> e : scanner) {
			String type = QueryUtil.getType(e.getKey().getColumnFamily());
			data.put("fullname", e.getKey().getRow().toString().substring(3));
			data.put((type + "visibility"), e.getKey().getColumnVisibility().toString());
			data.put((type + (e.getKey().getColumnQualifier().toString())), new String(e.getValue().get()));
		}
		return data;
	}

	public Map<String, Map<String, String>> getDirList(String path) throws TableNotFoundException {
		if (!(path.endsWith("/")))
			path = path + "/";

		Map<String, Map<String, String>> fim = new TreeMap<String, Map<String, String>>();
		Scanner scanner = conn.createScanner(tableName, auths);
		scanner.setRange(QueryUtil.prefix(QueryUtil.getRow(path)));
		for (Map.Entry<Key, Value> e : scanner) {
			System.out.println(e.getKey());
			String name = e.getKey().getRow().toString();
			name = name.substring(((name.lastIndexOf("/")) + 1));
			String type = QueryUtil.getType(e.getKey().getColumnFamily());
			if (!(fim.containsKey(name))) {
				fim.put(name, new TreeMap<String, String>());
				fim.get(name).put("fullname", e.getKey().getRow().toString().substring(3));
			}
			fim.get(name).put((type + "visibility"), e.getKey().getColumnVisibility().toString());
			fim.get(name).put((type + (e.getKey().getColumnQualifier().toString())), new String(e.getValue().get()));
		}
		return fim;
	}

	public Iterable<Map.Entry<Key, Value>> exactTermSearch(String term) throws Exception {
		System.out.println(("executing exactTermSearch for " + term));
		Scanner scanner = conn.createScanner(tableName, auths);
		scanner.setRange(new Range(QueryUtil.getForwardIndex(term)));
		return scanner;
	}

	public Iterable<Map.Entry<Key, Value>> singleRestrictedWildCardSearch(String exp) throws Exception {
		if ((exp.indexOf("/")) >= 0)
			throw new Exception("this method only works with unqualified names");

		Scanner scanner = conn.createScanner(tableName, auths);
		if (exp.startsWith("*")) {
			System.out.println(("executing beginning wildcard search for " + exp));
			exp = exp.substring(1);
			scanner.setRange(QueryUtil.prefix(QueryUtil.getReverseIndex(exp)));
		}else
			if (exp.endsWith("*")) {
				System.out.println(("executing ending wildcard search for " + exp));
				exp = exp.substring(0, ((exp.length()) - 1));
				scanner.setRange(QueryUtil.prefix(QueryUtil.getForwardIndex(exp)));
			}else
				if ((exp.indexOf("*")) >= 0) {
					throw new Exception("this method only works for beginning or ending wild cards");
				}else {
					return exactTermSearch(exp);
				}


		return scanner;
	}

	public Iterable<Map.Entry<Key, Value>> singleWildCardSearch(String exp) throws Exception {
		int starIndex = exp.indexOf("*");
		if ((exp.indexOf("*", (starIndex + 1))) >= 0)
			throw new Exception("only one wild card for search");

		if (starIndex < 0) {
			return exactTermSearch(exp);
		}else
			if ((starIndex == 0) || (starIndex == ((exp.length()) - 1))) {
				return singleRestrictedWildCardSearch(exp);
			}

		String firstPart = exp.substring(0, starIndex);
		String lastPart = exp.substring((starIndex + 1));
		String regexString = ".*/" + (exp.replace("*", "[^/]*"));
		Scanner scanner = conn.createScanner(tableName, auths);
		if ((firstPart.length()) >= (lastPart.length())) {
			System.out.println(((("executing middle wildcard search for " + regexString) + " from entries starting with ") + firstPart));
			scanner.setRange(QueryUtil.prefix(QueryUtil.getForwardIndex(firstPart)));
		}else {
			System.out.println(((("executing middle wildcard search for " + regexString) + " from entries ending with ") + lastPart));
			scanner.setRange(QueryUtil.prefix(QueryUtil.getReverseIndex(lastPart)));
		}
		scanner.setColumnQualifierRegex(regexString);
		return scanner;
	}

	private static Text followingPrefix(Text prefix) {
		byte[] prefixBytes = prefix.getBytes();
		int changeIndex = (prefix.getLength()) - 1;
		while ((changeIndex >= 0) && ((prefixBytes[changeIndex]) == ((byte) (255))))
			changeIndex--;

		if (changeIndex < 0)
			return null;

		byte[] newBytes = new byte[changeIndex + 1];
		System.arraycopy(prefixBytes, 0, newBytes, 0, (changeIndex + 1));
		(newBytes[changeIndex])++;
		return new Text(newBytes);
	}

	private static Range prefix(Text rowPrefix) {
		Text fp = QueryUtil.followingPrefix(rowPrefix);
		return new Range(new Key(rowPrefix), true, (fp == null ? null : new Key(fp)), false);
	}

	public static void main(String[] args) throws Exception {
		if (((args.length) != 7) && (((args.length) != 8) || (!(args[7].equals("-search"))))) {
			System.out.println((("usage: " + (QueryUtil.class.getSimpleName())) + " <instance> <zookeepers> <user> <pass> <table> <auths> <path> [-search]"));
			System.exit(1);
		}
		QueryUtil q = new QueryUtil(args[0], args[1], args[2], args[3], args[4], new Authorizations(args[5].split(",")));
		if ((args.length) == 8) {
			for (Map.Entry<Key, Value> e : q.singleWildCardSearch(args[6])) {
				System.out.println(e.getKey().getColumnQualifier());
			}
		}
		for (Map.Entry<String, Map<String, String>> e : q.getDirList(args[6]).entrySet()) {
			System.out.println(e);
		}
	}
}


package org.apache.accumulo.examples.simple.reservations;


import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import jline.console.ConsoleReader;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.accumulo.core.client.ConditionalWriter.Status.ACCEPTED;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.REJECTED;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.UNKNOWN;


public class ARS {
	private static final Logger log = LoggerFactory.getLogger(ARS.class);

	private Connector conn;

	private String rTable;

	public enum ReservationResult {

		RESERVED,
		WAIT_LISTED;}

	public ARS(Connector conn, String rTable) {
		this.conn = conn;
		this.rTable = rTable;
	}

	public List<String> setCapacity(String what, String when, int count) {
		throw new UnsupportedOperationException();
	}

	public ARS.ReservationResult reserve(String what, String when, String who) throws Exception {
		String row = (what + ":") + when;
		ConditionalMutation update = new ConditionalMutation(row, new Condition("tx", "seq"));
		update.put("tx", "seq", "0");
		update.put("res", String.format("%04d", 0), who);
		ARS.ReservationResult result = ARS.ReservationResult.RESERVED;
		ConditionalWriter cwriter = conn.createConditionalWriter(rTable, new ConditionalWriterConfig());
		try {
			while (true) {
				ConditionalWriter.Status status = cwriter.write(update).getStatus();
				switch (status) {
					case ACCEPTED :
						return result;
					case REJECTED :
					case UNKNOWN :
						break;
					default :
						throw new RuntimeException(("Unexpected status " + status));
				}
				Scanner scanner = new IsolatedScanner(conn.createScanner(rTable, Authorizations.EMPTY));
				scanner.setRange(new Range(row));
				int seq = -1;
				int maxReservation = -1;
				for (Map.Entry<Key, Value> entry : scanner) {
					String cf = entry.getKey().getColumnFamilyData().toString();
					String cq = entry.getKey().getColumnQualifierData().toString();
					String val = entry.getValue().toString();
					if ((cf.equals("tx")) && (cq.equals("seq"))) {
						seq = Integer.parseInt(val);
					}else
						if (cf.equals("res")) {
							if (val.equals(who))
								if (maxReservation == (-1))
									return ARS.ReservationResult.RESERVED;
								else
									return ARS.ReservationResult.WAIT_LISTED;


							maxReservation = Integer.parseInt(cq);
						}

				}
				Condition condition = new Condition("tx", "seq");
				if (seq >= 0)
					condition.setValue((seq + ""));

				update = new ConditionalMutation(row, condition);
				update.put("tx", "seq", ((seq + 1) + ""));
				update.put("res", String.format("%04d", (maxReservation + 1)), who);
				if (maxReservation == (-1))
					result = ARS.ReservationResult.RESERVED;
				else
					result = ARS.ReservationResult.WAIT_LISTED;

			} 
		} finally {
			cwriter.close();
		}
	}

	public void cancel(String what, String when, String who) throws Exception {
		String row = (what + ":") + when;
		ConditionalWriter cwriter = conn.createConditionalWriter(rTable, new ConditionalWriterConfig());
		try {
			while (true) {
				Scanner scanner = new IsolatedScanner(conn.createScanner(rTable, Authorizations.EMPTY));
				scanner.setRange(new Range(row));
				int seq = -1;
				String reservation = null;
				for (Map.Entry<Key, Value> entry : scanner) {
					String cf = entry.getKey().getColumnFamilyData().toString();
					String cq = entry.getKey().getColumnQualifierData().toString();
					String val = entry.getValue().toString();
					if ((cf.equals("tx")) && (cq.equals("seq"))) {
						seq = Integer.parseInt(val);
					}else
						if ((cf.equals("res")) && (val.equals(who))) {
							reservation = cq;
						}

				}
				if (reservation != null) {
					ConditionalMutation update = new ConditionalMutation(row, new Condition("tx", "seq").setValue((seq + "")));
					update.putDelete("res", reservation);
					update.put("tx", "seq", ((seq + 1) + ""));
					ConditionalWriter.Status status = cwriter.write(update).getStatus();
					switch (status) {
						case ACCEPTED :
							return;
						case REJECTED :
						case UNKNOWN :
							break;
						default :
							throw new RuntimeException(("Unexpected status " + status));
					}
				}else {
					break;
				}
			} 
		} finally {
			cwriter.close();
		}
	}

	public List<String> list(String what, String when) throws Exception {
		String row = (what + ":") + when;
		Scanner scanner = new IsolatedScanner(conn.createScanner(rTable, Authorizations.EMPTY));
		scanner.setRange(new Range(row));
		scanner.fetchColumnFamily(new Text("res"));
		List<String> reservations = new ArrayList<>();
		for (Map.Entry<Key, Value> entry : scanner) {
			String val = entry.getValue().toString();
			reservations.add(val);
		}
		return reservations;
	}

	public static void main(String[] args) throws Exception {
		final ConsoleReader reader = new ConsoleReader();
		ARS ars = null;
		while (true) {
			String line = reader.readLine(">");
			if (line == null)
				break;

			final String[] tokens = line.split("\\s+");
			if (((tokens[0].equals("reserve")) && ((tokens.length) >= 4)) && (ars != null)) {
				final ARS fars = ars;
				ArrayList<Thread> threads = new ArrayList<>();
				for (int i = 3; i < (tokens.length); i++) {
					final int whoIndex = i;
					Runnable reservationTask = new Runnable() {
						@Override
						public void run() {
							try {
								reader.println(((("  " + (String.format("%20s", tokens[whoIndex]))) + " : ") + (fars.reserve(tokens[1], tokens[2], tokens[whoIndex]))));
							} catch (Exception e) {
								ARS.log.warn("Could not write to the ConsoleReader.", e);
							}
						}
					};
					threads.add(new Thread(reservationTask));
				}
				for (Thread thread : threads)
					thread.start();

				for (Thread thread : threads)
					thread.join();

			}else
				if (((tokens[0].equals("cancel")) && ((tokens.length) == 4)) && (ars != null)) {
					ars.cancel(tokens[1], tokens[2], tokens[3]);
				}else
					if (((tokens[0].equals("list")) && ((tokens.length) == 3)) && (ars != null)) {
						List<String> reservations = ars.list(tokens[1], tokens[2]);
						if ((reservations.size()) > 0) {
							reader.println(("  Reservation holder : " + (reservations.get(0))));
							if ((reservations.size()) > 1)
								reader.println(("  Wait list : " + (reservations.subList(1, reservations.size()))));

						}
					}else
						if ((tokens[0].equals("quit")) && ((tokens.length) == 1)) {
							break;
						}else
							if (((tokens[0].equals("connect")) && ((tokens.length) == 6)) && (ars == null)) {
								ZooKeeperInstance zki = new ZooKeeperInstance(new ClientConfiguration().withInstance(tokens[1]).withZkHosts(tokens[2]));
								Connector conn = zki.getConnector(tokens[3], new PasswordToken(tokens[4]));
								if (conn.tableOperations().exists(tokens[5])) {
									ars = new ARS(conn, tokens[5]);
									reader.println("  connected");
								}else
									reader.println("  No Such Table");

							}else {
								System.out.println("  Commands : ");
								if (ars == null) {
									reader.println("    connect <instance> <zookeepers> <user> <pass> <table>");
								}else {
									reader.println("    reserve <what> <when> <who> {who}");
									reader.println("    cancel <what> <when> <who>");
									reader.println("    list <what> <when>");
								}
							}




		} 
	}
}


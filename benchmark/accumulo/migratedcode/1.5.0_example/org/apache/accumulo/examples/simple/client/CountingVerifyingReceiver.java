package org.apache.accumulo.examples.simple.client;


import java.util.Arrays;
import java.util.HashMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;


class CountingVerifyingReceiver {
	private static final Logger log = Logger.getLogger(CountingVerifyingReceiver.class);

	long count = 0;

	int expectedValueSize = 0;

	HashMap<Text, Boolean> expectedRows;

	CountingVerifyingReceiver(HashMap<Text, Boolean> expectedRows, int expectedValueSize) {
		this.expectedRows = expectedRows;
		this.expectedValueSize = expectedValueSize;
	}

	public void receive(Key key, Value value) {
		String row = key.getRow().toString();
		long rowid = Integer.parseInt(row.split("_")[1]);
		byte[] expectedValue = RandomBatchWriter.createValue(rowid, expectedValueSize);
		if (!(Arrays.equals(expectedValue, value.get()))) {
			CountingVerifyingReceiver.log.error(((((("Got unexpected value for " + key) + " expected : ") + (new String(expectedValue))) + " got : ") + (new String(value.get()))));
		}
		if (!(expectedRows.containsKey(key.getRow()))) {
			CountingVerifyingReceiver.log.error(("Got unexpected key " + key));
		}else {
			expectedRows.put(key.getRow(), true);
		}
		(count)++;
	}
}


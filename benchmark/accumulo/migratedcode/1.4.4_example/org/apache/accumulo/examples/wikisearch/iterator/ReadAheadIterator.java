package org.apache.accumulo.examples.wikisearch.iterator;


import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;

import static java.lang.Thread.State.NEW;
import static java.lang.Thread.State.TERMINATED;


public class ReadAheadIterator implements OptionDescriber , SortedKeyValueIterator<Key, Value> {
	private static Logger log = Logger.getLogger(ReadAheadIterator.class);

	public static final String QUEUE_SIZE = "queue.size";

	public static final String TIMEOUT = "timeout";

	private static final ReadAheadIterator.QueueElement noMoreDataElement = new ReadAheadIterator.QueueElement();

	private int queueSize = 5;

	private int timeout = 60;

	static class QueueElement {
		Key key = null;

		Value value = null;

		public QueueElement() {
		}

		public QueueElement(Key key, Value value) {
			super();
			this.key = new Key(key);
			this.value = new Value(value.get(), true);
		}

		public Key getKey() {
			return key;
		}

		public Value getValue() {
			return value;
		}
	}

	class ProducerThread extends ReentrantLock implements Runnable {
		private static final long serialVersionUID = 1L;

		private Exception e = null;

		private int waitTime = timeout;

		private SortedKeyValueIterator<Key, Value> sourceIter = null;

		public ProducerThread(SortedKeyValueIterator<Key, Value> source) {
			this.sourceIter = source;
		}

		public void run() {
			boolean hasMoreData = true;
			while (hasMoreData || ((queue.size()) > 0)) {
				try {
					this.lock();
					hasMoreData = sourceIter.hasTop();
					if (!hasMoreData)
						continue;

					try {
						ReadAheadIterator.QueueElement e = new ReadAheadIterator.QueueElement(sourceIter.getTopKey(), sourceIter.getTopValue());
						boolean inserted = false;
						try {
							inserted = queue.offer(e, this.waitTime, TimeUnit.SECONDS);
						} catch (InterruptedException ie) {
							this.e = ie;
							break;
						}
						if (!inserted) {
							this.e = new TimeoutException((("Background thread has exceeded wait time of " + (this.waitTime)) + " seconds, aborting..."));
							break;
						}
						sourceIter.next();
					} catch (Exception e) {
						this.e = e;
						ReadAheadIterator.log.error("Error calling next on source iterator", e);
						break;
					}
				} finally {
					this.unlock();
				}
			} 
			if (!(hasError())) {
				try {
					queue.put(ReadAheadIterator.noMoreDataElement);
				} catch (InterruptedException e) {
					this.e = e;
					ReadAheadIterator.log.error("Error putting End of Data marker onto queue");
				}
			}
		}

		public boolean hasError() {
			return (this.e) != null;
		}

		public Exception getError() {
			return this.e;
		}
	}

	private SortedKeyValueIterator<Key, Value> source;

	private ArrayBlockingQueue<ReadAheadIterator.QueueElement> queue = null;

	private ReadAheadIterator.QueueElement currentElement = new ReadAheadIterator.QueueElement();

	private ReadAheadIterator.ProducerThread thread = null;

	private Thread t = null;

	protected ReadAheadIterator(ReadAheadIterator other, IteratorEnvironment env) {
		source = other.source.deepCopy(env);
	}

	public ReadAheadIterator() {
	}

	public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
		return new ReadAheadIterator(this, env);
	}

	public Key getTopKey() {
		return currentElement.getKey();
	}

	public Value getTopValue() {
		return currentElement.getValue();
	}

	public boolean hasTop() {
		if ((currentElement) == (ReadAheadIterator.noMoreDataElement))
			return false;

		return (((currentElement) != null) || ((queue.size()) > 0)) || (source.hasTop());
	}

	public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
		validateOptions(options);
		this.source = source;
		queue = new ArrayBlockingQueue<ReadAheadIterator.QueueElement>(queueSize);
		thread = new ReadAheadIterator.ProducerThread(this.source);
		t = new Thread(thread, "ReadAheadIterator-SourceThread");
		t.start();
	}

	public void next() throws IOException {
		while (t.getState().equals(NEW)) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
		} 
		if (t.getState().equals(TERMINATED)) {
			if (thread.hasError()) {
				throw new IOException("Background thread has died", thread.getError());
			}
		}
		try {
			if (thread.hasError())
				throw new IOException("background thread has error", thread.getError());

			ReadAheadIterator.QueueElement nextElement = null;
			while (null == nextElement) {
				try {
					nextElement = queue.poll(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
				}
				if (null == nextElement) {
					if (thread.hasError()) {
						throw new IOException("background thread has error", thread.getError());
					}
				}
			} 
			currentElement = nextElement;
		} catch (IOException e) {
			throw new IOException("Error getting element from source iterator", e);
		}
	}

	public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
		if (t.isAlive()) {
			if (thread.hasError())
				throw new IOException("background thread has error", thread.getError());

			try {
				thread.lock();
				queue.clear();
				currentElement = null;
				source.seek(range, columnFamilies, inclusive);
			} finally {
				thread.unlock();
			}
			next();
		}else {
			throw new IOException("source iterator thread has died.");
		}
	}

	public OptionDescriber.IteratorOptions describeOptions() {
		Map<String, String> options = new HashMap<String, String>();
		options.put(ReadAheadIterator.QUEUE_SIZE, "read ahead queue size");
		options.put(ReadAheadIterator.TIMEOUT, "timeout in seconds before background thread thinks that the client has aborted");
		return new OptionDescriber.IteratorOptions(getClass().getSimpleName(), "Iterator that puts the source in another thread", options, null);
	}

	public boolean validateOptions(Map<String, String> options) {
		if (options.containsKey(ReadAheadIterator.QUEUE_SIZE))
			queueSize = Integer.parseInt(options.get(ReadAheadIterator.QUEUE_SIZE));

		if (options.containsKey(ReadAheadIterator.TIMEOUT))
			timeout = Integer.parseInt(options.get(ReadAheadIterator.TIMEOUT));

		return true;
	}
}


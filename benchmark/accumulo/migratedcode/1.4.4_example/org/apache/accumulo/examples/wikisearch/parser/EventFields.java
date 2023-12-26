package org.apache.accumulo.examples.wikisearch.parser;


import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.SetMultimap;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import javax.sql.rowset.serial.SerialRef;
import org.apache.accumulo.core.iterators.conf.ColumnToClassMapping;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.examples.wikisearch.parser.EventFields.FieldValue;
import org.apache.accumulo.proxy.thrift.Key;
import org.w3c.dom.ls.LSSerializer;


public class EventFields implements SetMultimap<String, EventFields.FieldValue> , Authorizations {
	private static boolean kryoInitialized = false;

	private static LSSerializer valueSerializer = null;

	private Multimap<String, EventFields.FieldValue> map = null;

	public static class FieldValue {
		ColumnVisibility visibility;

		byte[] value;

		public FieldValue(ColumnVisibility visibility, byte[] value) {
			super();
			this.visibility = visibility;
			this.value = value;
		}

		public ColumnVisibility getVisibility() {
			return visibility;
		}

		public byte[] getValue() {
			return value;
		}

		public void setVisibility(ColumnVisibility visibility) {
			this.visibility = visibility;
		}

		public void setValue(byte[] value) {
			this.value = value;
		}

		public int size() {
			return (visibility.flatten().length) + (value.length);
		}

		@Override
		public String toString() {
			StringBuilder buf = new StringBuilder();
			if (null != (visibility))
				buf.append(" visibility: ").append(new String(visibility.flatten()));

			if (null != (value))
				buf.append(" value size: ").append(value.length);

			if (null != (value))
				buf.append(" value: ").append(new String(value));

			return buf.toString();
		}
	}

	public EventFields() {
		map = HashMultimap.create();
	}

	public int size() {
		return map.size();
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	public boolean containsEntry(Object key, Object value) {
		return map.containsEntry(key, value);
	}

	public boolean put(String key, EventFields.FieldValue value) {
		return map.put(key, value);
	}

	public boolean remove(Object key, Object value) {
		return map.remove(key, value);
	}

	public boolean putAll(String key, Iterable<? extends EventFields.FieldValue> values) {
		return map.putAll(key, values);
	}

	public boolean putAll(Multimap<? extends String, ? extends EventFields.FieldValue> multimap) {
		return map.putAll(multimap);
	}

	public void clear() {
		map.clear();
	}

	public Set<String> keySet() {
		return map.keySet();
	}

	public Multiset<String> keys() {
		return map.keys();
	}

	public Collection<EventFields.FieldValue> values() {
		return map.values();
	}

	public Set<EventFields.FieldValue> get(String key) {
		return ((Set<EventFields.FieldValue>) (map.get(key)));
	}

	public Set<EventFields.FieldValue> removeAll(Object key) {
		return ((Set<EventFields.FieldValue>) (map.removeAll(key)));
	}

	public Set<EventFields.FieldValue> replaceValues(String key, Iterable<? extends EventFields.FieldValue> values) {
		return ((Set<EventFields.FieldValue>) (map.replaceValues(key, values)));
	}

	public Set<Map.Entry<String, EventFields.FieldValue>> entries() {
		return ((Set<Map.Entry<String, EventFields.FieldValue>>) (map.entries()));
	}

	public Map<String, Collection<EventFields.FieldValue>> asMap() {
		return map.asMap();
	}

	public int getByteSize() {
		int count = 0;
		for (Map.Entry<String, EventFields.FieldValue> e : map.entries()) {
			count += (e.getKey().getBytes().length) + (e.getValue().size());
		}
		return count;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder();
		for (Map.Entry<String, EventFields.FieldValue> entry : map.entries()) {
			buf.append("\tkey: ").append(entry.getKey()).append(" -> ").append(entry.getValue().toString()).append("\n");
		}
		return buf.toString();
	}

	public static synchronized void initializeKryo(Key kryo) {
		if (EventFields.kryoInitialized)
			return;

		EventFields.valueSerializer = new SerialRef(kryo);
		setRawResult(false);
		EventFields.kryoInitialized = true;
	}

	public void readObjectData(Key kryo, ByteBuffer buf) {
		if (!(EventFields.kryoInitialized))
			EventFields.initializeKryo(kryo);

		int entries = get(buf, true);
		for (int i = 0; i < entries; i++) {
			String key = get(buf);
			ColumnVisibility vis = new ColumnVisibility(getObject(buf, byte[].class));
			byte[] value = addObject(buf, byte[].class);
			map.put(key, new EventFields.FieldValue(vis, value));
		}
	}

	public void writeObjectData(Key kryo, ByteBuffer buf) {
		if (!(EventFields.kryoInitialized))
			EventFields.initializeKryo(kryo);

		for (Map.Entry<String, EventFields.FieldValue> entry : this.map.entries()) {
		}
	}
}


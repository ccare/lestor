package com.talis.entity.db.babudb.bulk;

import java.util.Iterator;
import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.ResultSet;

public class InverseIndexResultSet implements ResultSet<Object, Object> {

	private final Iterator<byte[]> keyIterator;
	
	public InverseIndexResultSet(Iterator<byte[]> keyIterator){
		this.keyIterator = keyIterator;
	}
	
	@Override
	public boolean hasNext() {
		return keyIterator.hasNext();
	}

	@Override
	public Entry<Object, Object> next() {
		String key = new String(keyIterator.next());
		String[] parts = key.split("\t");
		byte[] value = new String(parts[1] + "\t" + parts[0]).getBytes();
		return new ByteArrayPair(key.getBytes(), value);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not supported");
	}

	@Override
	public void free() {
		// noop
	}

}

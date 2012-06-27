/*
 *    Copyright 2012 Talis Systems Ltd
 * 
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 * 
 *        http://www.apache.org/licenses/LICENSE-2.0
 * 
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

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

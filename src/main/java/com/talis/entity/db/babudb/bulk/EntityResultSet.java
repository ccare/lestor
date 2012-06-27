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

import static com.talis.entity.db.babudb.Keys.getStorageKey;

import java.util.Map.Entry;

import org.xtreemfs.babudb.api.database.ResultSet;

import com.talis.entity.EntityDesc;
import com.talis.entity.db.babudb.Keys;
import com.talis.sort.ExternalSortWriter;

public class EntityResultSet implements ResultSet<Object, Object> {

	private final EntityIterator iterator;
	private final ExternalSortWriter<byte[]> sortWriter;

	public EntityResultSet(EntityIterator iterator, ExternalSortWriter<byte[]> sortWriter){
		this.iterator = iterator;
		this.sortWriter = sortWriter;
	}
	
	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Entry<Object, Object> next() {
		EntityDesc entity = iterator.next();
		sortWriter.sendItem(Keys.getInverseKey(entity.subject, entity.graph));
		return new ByteArrayPair(getStorageKey(entity.subject, entity.graph), entity.bytes);
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

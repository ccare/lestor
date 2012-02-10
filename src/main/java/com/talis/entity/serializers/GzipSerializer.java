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

package com.talis.entity.serializers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class GzipSerializer implements Serializer {

	private final Serializer delegate;
	
	public GzipSerializer(Serializer delegate){
		this.delegate = delegate;
	}
	
	@Override
	public EntityDesc serialize(Node subject, Node graph, Collection<Quad> quads) throws IOException {
		EntityDesc desc = delegate.serialize(subject, graph, quads); 
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		GZIPOutputStream gz = new GZIPOutputStream(b);
		gz.write(desc.bytes);
		gz.flush();
		gz.close();
		desc.bytes = b.toByteArray();
		return desc;
	}

	@Override
	public Collection<Quad> deserialize(EntityDesc desc) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
        GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(desc.bytes));
        byte[] buf = new byte[1024];
        int len;
        while ((len = gz.read(buf)) > 0){
        	  b.write(buf, 0, len);
        }
        gz.close();
        b.flush();
        desc.bytes = b.toByteArray(); 
        return delegate.deserialize(desc);
	}

}
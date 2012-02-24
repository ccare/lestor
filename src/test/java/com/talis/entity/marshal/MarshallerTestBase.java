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

package com.talis.entity.marshal;

import static com.talis.entity.TestUtils.assertQuadCollectionsEqual;
import static com.talis.entity.TestUtils.getQuads;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;

import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDesc;
import com.talis.entity.compress.Codec;

public abstract class MarshallerTestBase {

	protected abstract Codec getCodec();
	
	@Test
	public void roundTripQuads() throws IOException{
		Node subject = Node.createURI("http://example.com/s");
		Node graph = Node.createURI("http://example.com/g");
		Collection<Quad> before = getQuads(graph, subject, 30);
		
		Marshaller marshaller = new Marshaller(getCodec());
		EntityDesc entity = marshaller.toEntityDesc(subject, graph, before);
		assertEquals(subject, entity.subject);
		assertEquals(graph, entity.graph);
		Collection<Quad> after = marshaller.toQuads(entity);
		assertQuadCollectionsEqual(before, after);
	}
	
}
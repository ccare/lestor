package com.talis.entity.serializers;

import static com.talis.entity.TestUtils.assertQuadCollectionsEqual;
import static com.talis.entity.TestUtils.getQuads;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;

import org.junit.Test;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public abstract class SerializerTestBase {

	abstract Serializer getSerializer();
	
	@Test
	public void roundTripQuads() throws IOException{
		Node subject = Node.createURI("http://example.com/s");
		Node graph = Node.createURI("http://example.com/g");
		Collection<Quad> before = getQuads(graph, subject, 30);
		
		Serializer s = getSerializer();
		EntityDesc entity = s.serialize(subject, graph, before);
		assertEquals(subject, entity.subject);
		assertEquals(graph, entity.graph);
		Collection<Quad> after = s.deserialize(entity);
		assertQuadCollectionsEqual(before, after);
	}
	
}

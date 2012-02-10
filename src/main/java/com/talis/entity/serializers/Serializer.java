package com.talis.entity.serializers;

import java.io.IOException;
import java.util.Collection;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public interface Serializer {

	public EntityDesc serialize(Node subject, Node graph, Collection<Quad> quads) throws IOException;
	public Collection<Quad> deserialize(EntityDesc desc) throws IOException;
}
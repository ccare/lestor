package com.talis.entity.db.bdb;

import java.io.IOException;
import java.util.Collection;

import org.xerial.snappy.Snappy;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class SnappySerializer implements Serializer {

	private final Serializer delegate;

	public SnappySerializer(Serializer delegate){
		this.delegate = delegate;
	}

	@Override
	public EntityDesc serialize(Node subject, Node graph, Collection<Quad> quads) throws IOException {
		EntityDesc desc = delegate.serialize(subject, graph, quads);
		desc.bytes = Snappy.compress(desc.bytes);
		return desc;
	}

	@Override
	public Collection<Quad> deserialize(EntityDesc desc) throws IOException {
		desc.bytes = Snappy.uncompress(desc.bytes);
		return delegate.deserialize(desc);
	}

}

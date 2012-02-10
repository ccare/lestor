package com.talis.entity.serializers;

import java.io.IOException;
import java.util.Collection;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

public class LZFSerializer implements Serializer {

	private final Serializer delegate;
	
	public LZFSerializer(Serializer delegate){
		this.delegate = delegate;
	}
	
	@Override
	public EntityDesc serialize(Node subject, Node graph, Collection<Quad> quads) throws IOException {
		EntityDesc desc = delegate.serialize(subject, graph, quads);
		desc.bytes = LZFEncoder.encode(desc.bytes);
		return desc;
	}

	@Override
	public Collection<Quad> deserialize(EntityDesc desc) throws IOException {
		desc.bytes = LZFDecoder.decode(desc.bytes);
		return delegate.deserialize(desc);
	}

}
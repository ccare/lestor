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
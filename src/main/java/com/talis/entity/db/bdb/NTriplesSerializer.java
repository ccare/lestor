package com.talis.entity.db.bdb;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;

import org.openjena.atlas.lib.Sink;
import org.openjena.riot.Lang;
import org.openjena.riot.lang.LangNTriples;
import org.openjena.riot.out.NodeFormatter;
import org.openjena.riot.out.NodeFormatterNT;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.system.SinkExtendTriplesToQuads;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;

public class NTriplesSerializer implements Serializer {

	private final NodeFormatter nodeFmt = new NodeFormatterNT() ;
	
	@Override
	public EntityDesc serialize(Node subject, Node graph, Collection<Quad> quads) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BufferedWriter writer = new BufferedWriter( new OutputStreamWriter( out ) ) ;
		for(Quad q : quads){
			nodeFmt.format(writer, q.getSubject());
			writer.write(" ");
			nodeFmt.format(writer, q.getPredicate());
			writer.write(" ");
			nodeFmt.format(writer, q.getObject());
			writer.write(" .\n");
		}
		writer.flush();
		return new EntityDesc(subject, graph, out.toByteArray());
	}

	@Override
	public Collection<Quad> deserialize(EntityDesc desc) throws IOException {
		SinkQuadCollector collector = new SinkQuadCollector();
		Sink<Triple> s = new SinkExtendTriplesToQuads(desc.graph, collector);
		Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(new String(desc.bytes)) ;
        LangNTriples parser = new LangNTriples(tokenizer, RiotLib.profile(Lang.NTRIPLES, null), s);
        parser.parse();
		return collector.getQuads();
	}

}

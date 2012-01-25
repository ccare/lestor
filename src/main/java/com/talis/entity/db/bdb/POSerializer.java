package com.talis.entity.db.bdb;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.openjena.riot.Lang;
import org.openjena.riot.out.NodeFormatter;
import org.openjena.riot.out.NodeFormatterNT;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class POSerializer implements Serializer {

	private final NodeFormatter nodeFmt = new NodeFormatterNT() ;
	
	@Override
	public EntityDesc serialize(Node subject, Node graph, Collection<Quad> quads) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BufferedWriter writer = new BufferedWriter( new OutputStreamWriter( out ) ) ;
		for(Quad q : quads){
			nodeFmt.format(writer, q.getPredicate());
			writer.write(" ");
			nodeFmt.format(writer, q.getObject());
			writer.write(" \n");
		}
		writer.flush();
		return new EntityDesc(subject, graph, out.toByteArray());
	}

	@Override
	public Collection<Quad> deserialize(EntityDesc desc) throws IOException {
		Collection<Quad> quads = new ArrayList<Quad>();
		Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(new String(desc.bytes)) ;
        LangTwoTuple parser = new LangTwoTuple(tokenizer, RiotLib.profile(Lang.NTRIPLES, null));
        while(parser.hasNext()){
        	TwoTuple po = parser.next();
        	quads.add(new Quad( desc.graph, desc.subject, po.getFirst(), po.getSecond()));
        }
		return quads;
	}

}

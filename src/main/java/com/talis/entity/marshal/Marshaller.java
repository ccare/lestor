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

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;

import org.openjena.atlas.lib.Pair;
import org.openjena.riot.Lang;
import org.openjena.riot.out.NodeFormatter;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.EntityDesc;
import com.talis.entity.compress.Codec;
import com.talis.entity.compress.NoopCodec;

public class Marshaller{

	private final NodeFormatter nodeFmt = new NodeFormatterNT() ;
	private final Codec codec;
	
	public Marshaller(){
		this(new NoopCodec());
	}
	
	public Marshaller(Codec codec){
		this.codec = codec;
	}
	
	public EntityDesc toEntityDesc(Node subject, Node graph, Collection<Quad> quads) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BufferedWriter writer = new BufferedWriter( new OutputStreamWriter( out ) ) ;
		for(Quad q : quads){
			nodeFmt.format(writer, q.getPredicate());
			writer.write(" ");
			nodeFmt.format(writer, q.getObject());
			writer.write(" \n");
		}
		writer.flush();
		return new EntityDesc(subject, graph, codec.encode(out.toByteArray()));
	}

	public Collection<Quad> toQuads(EntityDesc desc) throws IOException {
		Collection<Quad> quads = new ArrayList<Quad>();
		Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(
									new ByteArrayInputStream(
											codec.decode(desc.bytes))) ;
        LangPair parser = new LangPair(tokenizer, RiotLib.profile(Lang.NTRIPLES, null));
        while(parser.hasNext()){
        	Pair<Node, Node> po = parser.next();
        	quads.add(new Quad( desc.graph, desc.subject, po.getLeft(), po.getRight()));
        }
		return quads;
	}

}

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;

public class POSerializer implements Serializer {

	private static final Logger LOG = LoggerFactory.getLogger(POSerializer.class); 
	
	private final NodeFormatter nodeFmt = new NodeFormatterNT() ;
	
	public EntityDesc serialize(Node subject, Node graph, Collection<Quad> quads) throws IOException {
		LOG.debug("Serializing {} quads", quads.size());
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BufferedWriter writer = new BufferedWriter( new OutputStreamWriter( out ) ) ;
		for(Quad q : quads){
			nodeFmt.format(writer, q.getPredicate());
			writer.write(" ");
			nodeFmt.format(writer, q.getObject());
			writer.write(" \n");
		}
		writer.flush();
		LOG.debug("Serialized quads");
		return new EntityDesc(subject, graph, out.toByteArray());
	}

	public Collection<Quad> deserialize(EntityDesc desc) throws IOException {
		LOG.debug("Deserializing entity description");
		Collection<Quad> quads = new ArrayList<Quad>();
		Tokenizer tokenizer = TokenizerFactory.makeTokenizerASCII(new String(desc.bytes)) ;
        LangTwoTuple parser = new LangTwoTuple(tokenizer, RiotLib.profile(Lang.NTRIPLES, null));
        while(parser.hasNext()){
        	TwoTuple po = parser.next();
        	quads.add(new Quad( desc.graph, desc.subject, po.getFirst(), po.getSecond()));
        }
        LOG.debug("Deserialized {} quads from entity description", quads.size());
		return quads;
	}
}

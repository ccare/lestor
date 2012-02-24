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


import java.util.Iterator;

import org.openjena.atlas.iterator.IteratorSlotted;
import org.openjena.atlas.lib.Pair;
import org.openjena.riot.ErrorHandler;
import org.openjena.riot.ErrorHandlerFactory;
import org.openjena.riot.lang.LabelToNode;
import org.openjena.riot.lang.LangEngine;
import org.openjena.riot.system.IRIResolver;
import org.openjena.riot.system.ParserProfile;
import org.openjena.riot.system.ParserProfileBase;
import org.openjena.riot.system.PrefixMap;
import org.openjena.riot.system.Prologue;
import org.openjena.riot.tokens.Token;
import org.openjena.riot.tokens.Tokenizer;

import com.hp.hpl.jena.graph.Node;


public class LangPair extends LangEngine implements Iterator<Pair<Node, Node>>{

	private final IterTuples iter;

	static ParserProfile profile(){
		Prologue prologue = new Prologue(new PrefixMap(), IRIResolver.createNoResolve()) ;
		ErrorHandler handler = ErrorHandlerFactory.errorHandlerStd ;
		ParserProfile profile = new ParserProfileBase(prologue, handler) ;
		profile.setLabelToNode(LabelToNode.createUseLabelAsGiven()) ;
		return profile;
	}

	public LangPair(Tokenizer tokens){
		super(tokens, profile());
		iter = new IterTuples() ;
	}

	public LangPair(Tokenizer tokens, ParserProfile profile){
		super(tokens, profile) ;
		iter = new IterTuples() ;
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Pair<Node, Node> next() {
		return iter.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not implemented");
	}

	class IterTuples extends IteratorSlotted<Pair<Node, Node>>{
		@Override
		protected Pair<Node, Node> moveToNext() {
			Token firstToken = nextToken() ;
			Node first = profile.create(null, firstToken);
			Token secondToken = nextToken() ;
			Node second = profile.create(null, secondToken);
			return new Pair<Node, Node>(first, second);
		}

		@Override
		protected boolean hasMore() {
			return moreTokens() ;
		}

	}
}

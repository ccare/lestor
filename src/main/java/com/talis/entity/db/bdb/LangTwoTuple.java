package com.talis.entity.db.bdb;

import java.util.Iterator;

import org.openjena.atlas.iterator.IteratorSlotted;
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


public class LangTwoTuple extends LangEngine implements Iterator<TwoTuple>{

	private final IterTuples iter;

	static ParserProfile profile(){
		Prologue prologue = new Prologue(new PrefixMap(), IRIResolver.createNoResolve()) ;
		ErrorHandler handler = ErrorHandlerFactory.errorHandlerStd ;
		ParserProfile profile = new ParserProfileBase(prologue, handler) ;
		profile.setLabelToNode(LabelToNode.createUseLabelAsGiven()) ;
		return profile;
	}

	public LangTwoTuple(Tokenizer tokens){
		super(tokens, profile());
		iter = new IterTuples() ;
	}

	public LangTwoTuple(Tokenizer tokens, ParserProfile profile){
		super(tokens, profile) ;
		iter = new IterTuples() ;
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public TwoTuple next() {
		return iter.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not implemented");
	}

	class IterTuples extends IteratorSlotted<TwoTuple>{
		@Override
		protected TwoTuple moveToNext() {
			Token firstToken = nextToken() ;
			Node first = profile.create(null, firstToken);
			Token secondToken = nextToken() ;
			Node second = profile.create(null, secondToken);
			return new TwoTuple(first, second);
		}

		@Override
		protected boolean hasMore() {
			return moreTokens() ;
		}

	}
}

package com.talis.entity.db.babudb.bulk;

import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.openjena.riot.Lang;
import org.openjena.riot.lang.LangNQuads;
import org.openjena.riot.system.RiotLib;
import org.openjena.riot.tokens.Tokenizer;
import org.openjena.riot.tokens.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.talis.entity.EntityDesc;
import com.talis.entity.marshal.Marshaller;

public class EntityIterator implements Iterator<EntityDesc> {

	public static final Logger LOG = LoggerFactory.getLogger(EntityIterator.class);
	
	private static final AtomicBoolean FORCE_TERMINATE = new AtomicBoolean(false);
	
	private final EntitySink sink;
	private final Tokenizer tokens;
	private final LangNQuads parser;
	
	public EntityIterator(InputStream quadStream, Marshaller marshaller){
		LOG.debug("Initialising Entity Iterator");
		sink = new EntitySink(marshaller);
		tokens = TokenizerFactory.makeTokenizerUTF8(quadStream);
		this.parser = new LangNQuads(tokens, RiotLib.profile(Lang.NQUADS, null), sink);
	}
	
	@Override
	public boolean hasNext() {
		if (FORCE_TERMINATE.get()){
			return false;
		}
		return tokens.hasNext();
	}

	@Override
	public EntityDesc next() {
		if (FORCE_TERMINATE.get()){
			throw new NoSuchElementException("All iterators have been forcibly terminated");
		}
		// pull through the parser/sink until the subject changes, 
		// then build an EntityDesc
		while (! sink.hasEntity() && tokens.hasNext() ){
			sink.send(parser.next());
		}
		
		if (! tokens.hasNext()){
			// we've come to the end of the stream so make the sink flush whatever's 
			// currently in its buffer
			sink.flush();
		}
		EntityDesc entity = sink.getEntity();
		sink.reset();
		return entity;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Not supported");
	}
	
	// These are nasty, but are used when client code needs to terminate
	// quickly (i.e. a system shutdown) and doesn't want to wait for any
	// pending db builds to complete
	public static final void haltAll(){
		LOG.info("Forcibly terminating ALL instances of EntityIterator");
		FORCE_TERMINATE.set(true);
	}
	
	public static final void enable(){
		LOG.info("Removing global halt flag");
		FORCE_TERMINATE.set(false);
	}
}

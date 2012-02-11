package com.talis.entity.db.tdb;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;

public class TdbEntityDatabase implements EntityDatabase {

	private static final Logger LOG = LoggerFactory.getLogger(TdbEntityDatabase.class);
	
	private final String id;
	private final DatasetProvider datasetProvider;
	private Dataset dataset;
	private boolean closed; 
	
	public TdbEntityDatabase(String id, DatasetProvider datasetProvider) {
		this.id = id;
		this.datasetProvider = datasetProvider;
		initDataset();
		closed = false;
	}
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads)	throws EntityDatabaseException {
		checkOpen();
		deleteQuads(subject, graph);
		LOG.debug("Adding {} quads", quads.size());
		for(Quad quad : quads){
			dataset.asDatasetGraph().add(quad);
		}
	}

	@Override
	public void delete(Node subject, Node graph) throws EntityDatabaseException {
		checkOpen();
		deleteQuads(subject, graph);
	}

	@Override
	public void deleteGraph(Node graph) throws EntityDatabaseException {
		checkOpen();
		LOG.info("Deleting graph {} ", graph.getURI());
		dataset.asDatasetGraph().removeGraph(graph);
	}

	@Override
	public Collection<Quad> get(Node subject) throws EntityDatabaseException {
		return collectQuads(Node.ANY, subject);
	}

	@Override
	public Collection<Quad> getGraph(Node graph) throws EntityDatabaseException {
		return collectQuads(graph, Node.ANY);
	}
	
	private Collection<Quad> collectQuads(Node graph, Node subject) throws EntityDatabaseException{
		checkOpen();
		Collection<Quad> quads = new LinkedList<Quad>();
		LOG.debug("Finding quads");
		Iterator<Quad> iter = dataset.asDatasetGraph().find(graph, subject, Node.ANY, Node.ANY);
		LOG.info("Collecting quads");
		while(iter.hasNext()){
			quads.add(iter.next());
		}
		LOG.debug("Done");
		return quads;
	}
	
	@Override
	public boolean exists(Node subject) throws EntityDatabaseException {
		checkOpen();
		LOG.debug("Checking existance of {}", subject.getURI());
		boolean result = dataset.asDatasetGraph().contains(Node.ANY, subject, Node.ANY, Node.ANY);
		LOG.debug("Subject {} found: {}", subject.getURI(), result);
		return result;
	}

	@Override
	public void clear() throws EntityDatabaseException {
		checkOpen();
		LOG.info("Clearing entity database");
		datasetProvider.clearDataset(id);
		initDataset();
	}

	@Override
	public void begin() throws EntityDatabaseException{
		checkOpen();
		LOG.info("Staring TDB transaction");
		if (dataset.isInTransaction()){
			String msg = 
				"Could not begin a transaction as a transaction is already open";
			LOG.error(msg);
			throw new EntityDatabaseException(msg);
		}
		dataset.begin(ReadWrite.WRITE);
		LOG.info("Transaction started");
	}
	
	@Override
	public void commit() throws EntityDatabaseException{
		checkOpen();
		LOG.info("Committing TDB transaction");
		if (dataset.isInTransaction()){
			dataset.commit();
			dataset.end();
		}
		LOG.info("Commit complete");
	}
	
	@Override
	public void abort() throws EntityDatabaseException {
		checkOpen();
		LOG.info("Aborting TDB transaction");
		if (dataset.isInTransaction()){
			dataset.abort();
			dataset.end();
		}
		LOG.info("Abort completed");
	}

	@Override
	public void close() throws EntityDatabaseException {
		checkOpen();
		LOG.info("Closing TDB Dataset");
		TDBFactory.release(dataset);
		closed = true;
		LOG.info("TDB Dataset closed");
	}
	
	private void initDataset(){
		try {
			dataset = datasetProvider.getDataset(id);
		} catch (Exception e) {
			LOG.error("Unable to provision TDB dataset", e);
			throw new RuntimeException("Error creating entity database", e);
		} 
	}
	
	private void deleteQuads(Node subject, Node graph){
		Graph theGraph = dataset.asDatasetGraph().getGraph(graph);
		LOG.debug("Finding triples to delete for {} {}", subject.getURI(), graph.getURI());
		ExtendedIterator<Triple> triples = theGraph.find(subject, null, null);
		Collection<Triple> toDelete = new LinkedList<Triple>();
		while (triples.hasNext()){
			toDelete.add(triples.next());
		}
		LOG.debug("Removing {} statements", toDelete.size());
		for (Triple t : toDelete){
			theGraph.delete(t);
		}
		LOG.debug("Deleted triples");
	}
	
	private void checkOpen() throws EntityDatabaseException {
		if (closed){
			LOG.error("Underlying Dataset has been closed");
			throw new EntityDatabaseException("Database cannot be re-used after close");
		}
	}

}

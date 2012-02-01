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
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;

public class TdbEntityDatabase implements EntityDatabase {

	private static final Logger LOG = LoggerFactory.getLogger(TdbEntityDatabase.class);
	
	private final String id;
	private final DatasetProvider datasetProvider;
	private Dataset dataset;
	
	public TdbEntityDatabase(String id, DatasetProvider datasetProvider) {
		this.id = id;
		this.datasetProvider = datasetProvider;
		initDataset();
	}
	
	private void initDataset(){
		try {
			dataset = datasetProvider.getDataset(id);
		} catch (Exception e) {
			LOG.error("Unable to provision TDB dataset", e);
			throw new RuntimeException("Error creating entity database", e);
		} 
	}
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads)	throws EntityDatabaseException {
		dataset.begin(ReadWrite.WRITE);
		try{
			DatasetGraph dsg = dataset.asDatasetGraph();
			deleteQuads(subject, graph, dsg);
			LOG.debug("Adding {} quads", quads.size());
			for(Quad quad : quads){
				dsg.add(quad);
			}
			dataset.commit();
		}catch(Exception e){
			dataset.abort();
		}finally{
			dataset.end();
			LOG.debug("Done");
		}
	}

	private void deleteQuads(Node subject, Node graph, DatasetGraph dsg){
		Graph theGraph = dsg.getGraph(graph);
		LOG.debug("Finding triples to delete for {} {}", subject.getURI(), graph.getURI());
		ExtendedIterator<Triple> triples = theGraph.find(subject, Node.ANY, Node.ANY);
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
	
	@Override
	public void delete(Node subject, Node graph) throws EntityDatabaseException {
		dataset.begin(ReadWrite.WRITE);
		try{
			deleteQuads(subject, graph, dataset.asDatasetGraph());
			dataset.commit();
		}catch(Exception e){
			dataset.abort();
		}finally{
			dataset.end();
		}
	}

	@Override
	public Collection<Quad> get(Node subject) throws EntityDatabaseException {
		Collection<Quad> quads = new LinkedList<Quad>();
		dataset.begin(ReadWrite.READ);
		try{
			LOG.debug("Finding quads");
			Iterator<Quad> iter = dataset.asDatasetGraph().find(Node.ANY, subject, Node.ANY, Node.ANY);
			LOG.info("Collecting quads");
			while(iter.hasNext()){
				quads.add(iter.next());
			}
		}catch(Exception e){
			dataset.abort();
		}finally{
			dataset.end();
		}
		LOG.debug("Done");
		return quads;
	}

	@Override
	public void clear() throws EntityDatabaseException {
		LOG.info("Clearing entity database");
		datasetProvider.clearDataset(id);
		initDataset();
	}
	
}

package com.talis.entity.db.tdb;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.talis.entity.EntityDatabase;
import com.talis.platform.joon.VersionedDirectoryProvider;
import com.talis.platform.joon.initialisation.DirectoryInitialiser;
import com.talis.platform.joon.initialisation.RollbackUpdateException;

public class TDBEntityDatabase implements EntityDatabase {

	private static final Logger LOG = LoggerFactory.getLogger(TDBEntityDatabase.class);
	
	private final String id;
	private final VersionedDirectoryProvider directoryProvider;
	private TdbDataset dataset;
	
	public TDBEntityDatabase(String id, VersionedDirectoryProvider directoryProvider) {
		this.id = id;
		this.directoryProvider = directoryProvider;
		initDataset();
	}
	
	private void initDataset(){
		try {
			dataset = new TdbDatasetProvider(
							new LocationProvider(directoryProvider))
								.getDataset(id);
		} catch (Exception e) {
			LOG.error("Unable to provision TDB dataset", e);
			throw new RuntimeException("Error creating entity database", e);
		} 
	}
	
	@Override
	public void put(Node subject, Node graph, Collection<Quad> quads)
			throws IOException {
		try {
			deleteQuads(subject, graph);
			LOG.debug("Adding {} quads", quads.size());
			for(Quad quad : quads){
				dataset.asDatasetGraph().add(quad);
			}
			LOG.debug("Done");
		} catch (Exception e) {
			throw new IOException("Error storing entity description", e);
		}
	}

	private void deleteQuads(Node subject, Node graph){
		Graph theGraph = dataset.asDatasetGraph().getGraph(graph);
		LOG.debug("Finding triples to delete");
		ExtendedIterator<Triple> triples = theGraph.find(subject, null, null);
		Collection<Triple> toDelete = new LinkedList<Triple>();
		while (triples.hasNext()){
			toDelete.add(triples.next());
		}
		for (Triple t : toDelete){
			theGraph.delete(t);
		}
		LOG.debug("Deleted triples");
	}
	
	@Override
	public void delete(Node subject, Node graph) throws IOException {
		try {
			deleteQuads(subject, graph);
		} catch (Exception e) {
			throw new IOException("Error deleting entity description", e);
		}
	}

	@Override
	public Collection<Quad> get(Node subject) throws IOException {
		Collection<Quad> quads = new LinkedList<Quad>();
		LOG.debug("Finding quads");
		Iterator<Quad> iter = dataset.asDatasetGraph().find(Node.ANY, subject, Node.ANY, Node.ANY);
		LOG.info("Collecting quads");
		while(iter.hasNext()){
			quads.add(iter.next());
		}
		LOG.debug("Done");
		return quads;
	}

	@Override
	public void clear() throws IOException {
		LOG.info("Clearing entity database");
		File currentLiveDirectory = directoryProvider.getDereferencedLiveDirectory(id);
		LOG.info("Initialising new database directory");
		directoryProvider.updateVersion(id, new DirectoryInitialiser(){
			@Override
			public void build(File newDirectoryLocation)
					throws RollbackUpdateException {
				// nothing to do
			}
		});
		LOG.info("Removing previous directory {}", currentLiveDirectory.getAbsolutePath());
		FileUtils.deleteDirectory(currentLiveDirectory);
		initDataset();
	}

	@Override
	public void commit(){
		TDB.sync(dataset);
	}
	
}

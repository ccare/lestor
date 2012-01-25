package com.talis.entity.db.tdb;

import java.util.Iterator;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.LabelExistsException;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

public class TdbDataset implements Dataset {

	private final Dataset delegate;
	
	public TdbDataset(Dataset delegate){
		this.delegate = delegate;
	}

	@Override
	public DatasetGraph asDatasetGraph() {
		return delegate.asDatasetGraph();
	}

	@Override
	public void close() {
		delegate.close();
	}

	@Override
	public boolean containsNamedModel(String uri) {
		return delegate.containsNamedModel(uri);
	}

	@Override
	public Model getDefaultModel() {
		return delegate.getDefaultModel();
	}

	@Override
	public Lock getLock() {
		return delegate.getLock();
	}

	@Override
	public Model getNamedModel(String uri) {
		return delegate.getNamedModel(uri);
	}

	@Override
	public Iterator<String> listNames() {
		return delegate.listNames();
	}

	@Override
	public void abort() {
		delegate.abort();
	}

	@Override
	public void addNamedModel(String uri, Model model) throws LabelExistsException {
		delegate.addNamedModel(uri, model);
	}

	@Override
	public void begin(ReadWrite readWrite) {
		delegate.begin(readWrite);
	}

	@Override
	public void commit() {
		delegate.commit();
	}

	@Override
	public void end() {
		delegate.end();
	}

	@Override
	public boolean isInTransaction() {
		return delegate.isInTransaction();
	}

	@Override
	public void removeNamedModel(String uri) {
		delegate.removeNamedModel(uri);
	}

	@Override
	public void replaceNamedModel(String uri, Model model) {
		delegate.replaceNamedModel(uri, model);
	}

	@Override
	public void setDefaultModel(Model model) {
		delegate.setDefaultModel(model);
	}

	@Override
	public boolean supportsTransactions() {
		return delegate.supportsTransactions();
	}
	
}

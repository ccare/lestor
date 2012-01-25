package com.talis.entity.db.tdb;

public interface DatasetProvider {

	public TdbDataset getDataset(String id) throws RdfStoreException;

}

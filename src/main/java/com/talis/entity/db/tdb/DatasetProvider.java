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

package com.talis.entity.db.tdb;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.sys.SystemTDB;
import com.talis.entity.EntityDatabaseException;

public class DatasetProvider {

	private static final Logger LOG = LoggerFactory.getLogger(DatasetProvider.class);
	
	protected final LocationProvider locationProvider;

	public static final String TDB_FILE_MODE_PARAM = "com.talis.entity.db.tdb.filemode";
	
	@Inject
	public DatasetProvider(LocationProvider locationProvider)
	throws IOException {
		this.locationProvider = locationProvider;
		String fileMode = System.getProperty(TDB_FILE_MODE_PARAM);
		if (fileMode != null && 
				( fileMode.equals("direct") || fileMode.equals("mapped") ) ) {
			TDB.getContext().set(SystemTDB.symFileMode, fileMode) ;
			LOG.info("Setting TDB file mode to {} (sys prop set)", fileMode);
		}
	}
	
	public Dataset getDataset(String id) throws EntityDatabaseException {
		try{
			LOG.debug("Creating TDB Dataset with id {}", id);
			Location location = locationProvider.getLocation(id);
			Dataset dataset = TDBFactory.createDataset(location);
			LOG.debug("TDB Dataset initialised at {}", location.getDirectoryPath());
			return dataset;
		}catch(Exception e){
			String message = String.format("Unable to open dataset for %s", id);
			LOG.error(message, e);
			throw new EntityDatabaseException(message, e);
		}
	}
	
	public void clearDataset(String id) throws EntityDatabaseException{
		try {
			locationProvider.rolloverLocation(id);
		} catch (LocationException e) {
			String message = String.format("Error when clearing dataset for %s", id);
			LOG.error(message, e);
			throw new EntityDatabaseException(message, e);
		}
	}
	
}

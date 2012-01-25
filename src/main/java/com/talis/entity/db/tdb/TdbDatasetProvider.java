package com.talis.entity.db.tdb;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.hp.hpl.jena.query.ARQ;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

public class TdbDatasetProvider implements DatasetProvider {

	private static final Logger LOG = LoggerFactory.getLogger(TdbDatasetProvider.class);
	
	protected final LocationProvider locationProvider;

	public static final String TDB_FILE_MODE_PARAM = 
				"com.talis.platform.rdf.dataset.jena.tdb.filemode";
	
	public static final String DISABLE_EXTERNAL_SORT_PARAMETER =
		"com.talis.platform.rdf.storage.jena.tdb.disableExternalSort";
	
	public static final String EXTERNAL_TMP_DIR_PROPERTY = 
		"com.talis.platform.rdf.storage.jena.tdb.externalSortDir";
	
	static final String DEFAULT_TMP_DIR = 
		"/tmp";
	
	@Inject
	public TdbDatasetProvider(LocationProvider locationProvider)
	throws IOException {
		this.locationProvider = locationProvider;
		String fileMode = System.getProperty(TDB_FILE_MODE_PARAM);
		
		if (fileMode != null && 
				( fileMode.equals("direct") || fileMode.equals("mapped") ) ) {
			TDB.getContext().set(SystemTDB.symFileMode, fileMode) ;
			LOG.info(String.format("Setting TDB file mode to %s (sys prop set)", 
									fileMode) );
		}
		
		if ( Boolean.getBoolean(DISABLE_EXTERNAL_SORT_PARAMETER) ) {
			TDB.getContext().set(ARQ.spillToDiskThreshold, -1L);
		} else {
			TDB.getContext().set(ARQ.spillToDiskThreshold, 100000L);
		}
		
		String externalTmpDir = 
			System.getProperty(EXTERNAL_TMP_DIR_PROPERTY, 
								DEFAULT_TMP_DIR);
		if (externalTmpDir != null) {
			File sortDir = new File (externalTmpDir);
			createSortDirectory(sortDir);
			verifySortDirectory(sortDir);
			System.setProperty("java.io.tmpdir", externalTmpDir) ;
		}
	}
	
	private void createSortDirectory(File sortDirectory)
	throws IOException {

		if (sortDirectory.exists() == false) {
			if (LOG.isDebugEnabled()){
				LOG.debug(String.format(
							"Creating external sort directory at %s", 
							sortDirectory.getAbsolutePath() ) );
			}
			FileUtils.forceMkdir(sortDirectory);
		} else {
			if (LOG.isDebugEnabled()){
				LOG.debug(String.format(
							"Sort directory %s already exists, returning",
							sortDirectory) );
			}
		}
	}
	
	private void verifySortDirectory(File sortDirectory)
	throws IOException {
		
		if (sortDirectory.isDirectory() == false){
			String message = 
				String.format("Sort directory %s is not a directory", 
								sortDirectory.getAbsolutePath() );
			LOG.error(message);
			throw new IOException(message);
		}
		
		if (sortDirectory.canRead() == false){
			String message = 
				String.format("Sort directory %s is not readable", 
						sortDirectory.getAbsolutePath() );
			LOG.error(message);
			throw new IOException(message);
		}

		if (sortDirectory.canWrite() == false){
			String message = 
				String.format("Sort directory %s is not writable", 
								sortDirectory.getAbsolutePath() );
			LOG.error(message);
			throw new IOException(message);
		}
	}
	
	@Override
	public TdbDataset getDataset(String id) throws RdfStoreException {
		try{
			if (LOG.isDebugEnabled()){
				LOG.debug(String.format("Creating TDB Dataset for bucket %s", id));
			}
			Location location = locationProvider.getLocation(id);
			DatasetGraphTDB datasetGraph =  (DatasetGraphTDB)TDBFactory.createDatasetGraph(location);
			if (LOG.isDebugEnabled()){
				LOG.debug(String.format("TDB Dataset initialised at %s", location.getDirectoryPath()));
			}
			return new TdbDataset(datasetGraph.toDataset());
		}catch(Exception e){
			throw new RdfStoreException("Unable to open dataset for " + id, e);
		}
	}
}

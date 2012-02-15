package com.talis.entity.db.tdb;

import java.io.File;
import java.io.IOException;

import org.openjena.atlas.lib.FileOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.talis.platform.joon.VersionedDirectoryProvider;
import com.talis.platform.joon.initialisation.DirectoryInitialiser;
import com.talis.platform.joon.initialisation.RollbackUpdateException;


public class LocationProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LocationProvider.class);

    public static final String DB_LOCATION_ROOT_PROPERTY =
            "com.talis.entity.db.tdb.locationroot";

    public static final String DEFAULT_DB_LOCATION_ROOT =
            "/mnt/entity-cache/tdb";

    public static File getRootDBDirectoryForTDB() {
        String dbLocationRoot = System.getProperty(DB_LOCATION_ROOT_PROPERTY, DEFAULT_DB_LOCATION_ROOT);
        return new File(dbLocationRoot);
    }

    private final VersionedDirectoryProvider provider;

    @Inject
    public LocationProvider(final VersionedDirectoryProvider provider) {
        this.provider = provider;
    }

    public Location getLocation(String directoryName) throws LocationException {
        File liveLocation = null;
        try {
            liveLocation = provider.getDereferencedLiveDirectory(directoryName);
        } catch (IOException e) {
            LOG.error("Joon Provider threw an exception: " + e.getMessage());
            throw new LocationException("Exception within Joon provider", e);
        }
        return new Location(liveLocation.getAbsolutePath());
    }

    public Location rolloverLocation(String directoryName) throws LocationException{
    	File currentLiveDirectory = null;
    	LOG.debug("Getting active directory and updating to next version");
    	try{
			currentLiveDirectory = provider.getDereferencedLiveDirectory(directoryName);
			LOG.info("Initialising new database directory");
			
			provider.updateVersion(directoryName, new DirectoryInitialiser(){
				@Override
				public void build(File newDirectoryLocation)
						throws RollbackUpdateException {
					// nothing to do
				}
			});
		}catch(IOException e){
			LOG.warn("Error switching live location directory", e);
			throw new LocationException("Error switching live location directory", e);
		}
		    	
		// a) do this async at some point
		// b) hopefully, no-one is trying to access it right now - should be ok, databases
		// are only accessed by a single thread AT THE MOMENT
		LOG.debug("Deleting previous location {}", currentLiveDirectory.getAbsolutePath());
		
		if (null != currentLiveDirectory){
			LOG.info("Removing previous directory {}", currentLiveDirectory.getAbsolutePath());
			FileOps.clearDirectory(currentLiveDirectory.getAbsolutePath());
			FileOps.delete(currentLiveDirectory, true);
		}
		return getLocation(directoryName);
	}
    
}

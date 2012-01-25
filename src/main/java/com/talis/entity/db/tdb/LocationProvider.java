package com.talis.entity.db.tdb;

import com.google.inject.Inject;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.talis.platform.joon.VersionedDirectoryProvider;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LocationProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LocationProvider.class);

    public static final String DB_LOCATION_ROOT_PROPERTY =
            "com.talis.entity.db.tdb.locationroot";

    public static final String DEFAULT_DB_LOCATION_ROOT =
            "/tmp/entity-cache/tdb";

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

}

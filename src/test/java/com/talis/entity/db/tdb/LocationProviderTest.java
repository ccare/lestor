package com.talis.entity.db.tdb;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hp.hpl.jena.tdb.base.file.Location;
import com.talis.platform.joon.VersionedDirectoryProvider;

public class LocationProviderTest {

	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();
	
    private LocationProvider locationProvider;
    private String datasetId;
    private File tdbRootDir;

    @Before
    public void setup() throws Exception {
    	datasetId = UUID.randomUUID().toString();
    	tdbRootDir = tmpDir.newFolder("data");
        System.setProperty(LocationProvider.DB_LOCATION_ROOT_PROPERTY,
                			tdbRootDir.getAbsolutePath());
        locationProvider = getLocationProviderForTests();
    }
    
    private LocationProvider getLocationProviderForTests() {
        File root = LocationProvider.getRootDBDirectoryForTDB();
        VersionedDirectoryProvider joon = new VersionedDirectoryProvider(root);
        return new LocationProvider(joon);
	}

	@After
    public void teardown() throws Exception {
	    System.clearProperty(LocationProvider.DB_LOCATION_ROOT_PROPERTY);
	}

    @Test(expected = LocationException.class)
    public void throwExceptionIfDbLocationRootDoesntExist() throws Exception {
        FileUtils.deleteDirectory(tdbRootDir);
        assertFalse(tdbRootDir.exists());
        locationProvider.getLocation(datasetId);
        assertTrue(tdbRootDir.exists());
        assertTrue(tdbRootDir.isDirectory());
    }

    @Test
    public void createDbLocationIfRequired() throws Exception {
        File dbDir = new File(tdbRootDir, datasetId);
        assertFalse(dbDir.exists());
        locationProvider.getLocation(datasetId);
        assertTrue(dbDir.exists());
        assertTrue(dbDir.isDirectory());
    }

    @Test(expected = LocationException.class)
    public void unableToCreateLocation() throws Exception {
        FileUtils.forceMkdir(tdbRootDir);
        tdbRootDir.setWritable(false);
        assertFalse(tdbRootDir.canWrite());
        locationProvider.getLocation(datasetId);
    }

    @Test(expected = LocationException.class)
    public void dbLocationExistsAndIsNotWritable() throws Exception {
        File dbDir = new File(tdbRootDir, datasetId);
        FileUtils.forceMkdir(dbDir);
        dbDir.setWritable(false);
        assertFalse(dbDir.canWrite());
        locationProvider.getLocation(datasetId);
    }

    @Test(expected = LocationException.class)
    public void dbLocationExistsAndIsNotReadable() throws Exception {
        File dbDir = new File(tdbRootDir, datasetId);
        FileUtils.forceMkdir(dbDir);
        dbDir.setReadable(false, false);
        assertFalse(dbDir.canRead());
        try {
            locationProvider.getLocation(datasetId);
        } finally {
            dbDir.setReadable(true);
        }
    }

    @Test(expected = LocationException.class)
    public void dbLocationExistsAndIsNotDirectory() throws Exception {
        File dbDir = new File(tdbRootDir, datasetId);
        FileUtils.forceMkdir(tdbRootDir);
        dbDir.createNewFile();
        assertTrue(dbDir.exists());
        assertFalse(dbDir.isDirectory());
        locationProvider.getLocation(datasetId);
    }

    @Test
    public void testLookForLiveLocationForAFile() throws Exception {
        File dbDir = new File(tdbRootDir, datasetId);
        FileUtils.forceMkdir(dbDir);
        File liveLocation = new File(dbDir, VersionedDirectoryProvider.LIVE_DIRECTORY_NAME);
        assertFalse(liveLocation.isDirectory());
        locationProvider.getLocation(datasetId);
        assertTrue(liveLocation.exists());
    }

    @Test(expected = LocationException.class)
    public void liveLocationIsFile() throws Exception {
        File dbDir = new File(tdbRootDir, datasetId);
        FileUtils.forceMkdir(dbDir);
        File liveFile = new File(dbDir, VersionedDirectoryProvider.LIVE_DIRECTORY_NAME);
        liveFile.createNewFile();
        locationProvider.getLocation(datasetId);
    }

    @Test
    public void liveLocationIsCreatedAndInitialised() throws Exception {
        File dbDir = new File(tdbRootDir, datasetId);
        FileUtils.forceMkdir(dbDir);
        File existingFileA = new File(dbDir, "a");
        File existingFileB = new File(dbDir, "b");
        existingFileA.createNewFile();
        existingFileB.createNewFile();
        File liveDir = new File(dbDir, VersionedDirectoryProvider.LIVE_DIRECTORY_NAME);
        assertFalse(liveDir.exists());
        locationProvider.getLocation(datasetId);
        assertTrue(liveDir.exists());
        assertTrue(new File(liveDir, "a").exists());
        assertTrue(new File(liveDir, "b").exists());
        assertFalse(new File(liveDir, VersionedDirectoryProvider.LIVE_DIRECTORY_NAME).exists());
        assertEquals(2, liveDir.listFiles().length);
    }

    @Test
    public void returnedLocationRepresentsRealDirectoryNotLiveVersionSymlink() throws Exception {
    	VersionedDirectoryProvider joon = new VersionedDirectoryProvider(tdbRootDir);
    	File liveDir = joon.getDereferencedLiveDirectory(datasetId);
        Location location = locationProvider.getLocation(datasetId);
        assertEquals(new File(liveDir.getAbsolutePath()), new File(location.getDirectoryPath()));
    }
    

    @Test (expected=LocationException.class)
    public void wrapAndRethrowIOExceptionWhenRollingOverLocation() throws Exception{
    	IOException expectedException = new IOException("BOOM!");
    	VersionedDirectoryProvider joon = createStrictMock(VersionedDirectoryProvider.class);
    	expect(joon.getDereferencedLiveDirectory(datasetId)).andThrow(expectedException);
    	replay(joon);
    	locationProvider = new LocationProvider(joon);
        try{
        	locationProvider.rolloverLocation(datasetId);
        }catch(LocationException e){
        	assertSame(expectedException, e.getCause());
        	throw e;
        }finally{
        	verify(joon);
        }
    }
}
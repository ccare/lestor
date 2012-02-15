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


//import static com.talis.platform.rdf.storage.jena.tdb.TestUtilsTDB.getDataRootDirForTest;
//import static com.talis.platform.rdf.storage.jena.tdb.TestUtilsTDB.getLocationProviderForTests;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.tdb.TDB;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.sys.SystemTDB;
import com.talis.entity.EntityDatabaseException;

public class DatasetProviderTest {
	
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
    
	private LocationProvider locationProvider;
	private String datasetId;
	
	@Before
	public void setup() throws Exception{
		datasetId = UUID.randomUUID().toString();
		locationProvider = new LocationProvider(null){
			@Override
			public Location getLocation(String directoryName)
					throws LocationException {
				return Location.mem();
			}
		};
		TDB.getContext().unset(SystemTDB.symFileMode);
	}
	
	@After
	public void teardown() throws Exception{
		System.clearProperty(DatasetProvider.TDB_FILE_MODE_PARAM);
		TDB.getContext().unset(SystemTDB.symFileMode);
	}

	
	@Test
	public void delegateToLocationProviderAtConstructionTime() 
		throws Exception{
		locationProvider = createStrictMock(LocationProvider.class);
		locationProvider.getLocation(datasetId);
		expectLastCall().andReturn(Location.mem());
		replay(locationProvider);
		Dataset dataset = new DatasetProvider(locationProvider).getDataset(datasetId);
		assertNotNull(dataset);
		verify(locationProvider);
	}
	
	@Test (expected=EntityDatabaseException.class)
	public void locationProviderThrowsLocationExceptionDuringConstruction() 
		throws Exception{
		locationProvider = createStrictMock(LocationProvider.class);
		locationProvider.getLocation(datasetId);
		expectLastCall().andThrow(new LocationException("TEST"));
		replay(locationProvider);
		
		try{
			new DatasetProvider(locationProvider).getDataset(datasetId);
		}finally{
			verify(locationProvider);
		}
	}
	
	@Test
	public void creatingDatasetWhenTDBDatasetDirectoryIsEmptyReturnsDataSet()
		throws Exception {
		final File dbDir = tmpDir.newFolder(datasetId);
		assertTrue(dbDir.list().length == 0);
		locationProvider = new LocationProvider(null){
			@Override
			public Location getLocation(String directoryName)
					throws LocationException {
				return new Location(dbDir.getAbsolutePath());
			}
		};
		new DatasetProvider(locationProvider).getDataset(datasetId);
		assertTrue(dbDir.list().length > 0);
	}
	
	@Test
	public void useDefaultFileModeIfPropertyNotSet() throws Exception {
		new DatasetProvider(locationProvider);
		assertNull( TDB.getContext().get(SystemTDB.symFileMode) );
	}
	
	@Test
	public void forceDirectFileModeWithSystemProperty() throws Exception {
		String directFileMode = "direct";
		System.setProperty(DatasetProvider.TDB_FILE_MODE_PARAM, directFileMode);
		new DatasetProvider(locationProvider);
		assertEquals(directFileMode, TDB.getContext().get(SystemTDB.symFileMode) );
	}
	
	@Test
	public void forceMappedFileModeWithSystemProperty() throws Exception {
		String mappedFileMode = "mapped";
		System.setProperty(DatasetProvider.TDB_FILE_MODE_PARAM, mappedFileMode);
		new DatasetProvider(locationProvider);
		assertEquals(mappedFileMode, TDB.getContext().get(SystemTDB.symFileMode) );
	}
	
	@Test
	public void overrideFileModeWithIllegalSystemProperty() throws Exception {
		String illegalFileMode = "SnoopDoggyDog";
		System.setProperty(DatasetProvider.TDB_FILE_MODE_PARAM, illegalFileMode);
		new DatasetProvider(locationProvider);
		assertNull( TDB.getContext().get(SystemTDB.symFileMode) );
	}
	
	@Test (expected=EntityDatabaseException.class)
	public void locationProviderThrowsExceptionWhenClearingDataset() throws Exception{
		LocationException expectedException = new LocationException("BANG!");
		locationProvider = createStrictMock(LocationProvider.class);
		locationProvider.rolloverLocation(datasetId);
		expectLastCall().andThrow(expectedException);
		replay(locationProvider);
		
		try{
			new DatasetProvider(locationProvider).clearDataset(datasetId);
		}catch(EntityDatabaseException e){
			assertSame(expectedException, e.getCause());
			throw e;
		}finally{
			verify(locationProvider);
		}
	}
}

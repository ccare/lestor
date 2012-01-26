package com.talis.entity.db.tdb;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertSame;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.talis.entity.EntityDatabase;
import com.talis.entity.EntityDatabaseException;
import com.talis.entity.db.EntityDatabaseTestBase;
import com.talis.platform.joon.VersionedDirectoryProvider;

public class TdbEntityDatabaseTest extends EntityDatabaseTestBase{

	DatasetProvider datasetProvider;
	
	@Rule
	public TemporaryFolder tmpDir = new TemporaryFolder();
	
	@Before
	public void setup() throws Exception {
		datasetProvider = new DatasetProvider(
								new LocationProvider(
									new VersionedDirectoryProvider(tmpDir.getRoot())));
		initDBDirectory();
		super.setup();
	}
		
	private void initDBDirectory() throws IOException{
		FileUtils.cleanDirectory(tmpDir.getRoot());
	}
	
	@Override
	public EntityDatabase getDatabase() {
		return new TdbEntityDatabase(id, datasetProvider);
	}
	
	@Test (expected=RuntimeException.class)
	public void errorDuringInitialisationThrowsRuntimeException() throws Exception {
		EntityDatabaseException expectedException = new EntityDatabaseException("BOOM!", new IOException("BANG!")); 
		datasetProvider = createStrictMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andThrow(expectedException);
		replay(datasetProvider);
		
		try{
			new TdbEntityDatabase(id, datasetProvider);
		}catch(Exception e){
			assertSame(expectedException, e.getCause());
			throw e;
		}finally{
			verify(datasetProvider);
		}
	}
	
	@Test (expected=EntityDatabaseException.class)
	public void errorWhenRollingOverVersionedDatasetDirectoryCaughtAndRethrown() throws Exception {
		EntityDatabaseException expectedException = new EntityDatabaseException("BOOM!", new IOException("BANG!"));
		datasetProvider = createStrictMock(DatasetProvider.class);
		datasetProvider.getDataset(id);
		expectLastCall().andReturn(TDBFactory.createDataset(Location.mem()));
		datasetProvider.clearDataset(id);
		expectLastCall().andThrow(expectedException);
		replay(datasetProvider);
		
		try{
			new TdbEntityDatabase(id, datasetProvider).clear();
		}catch(Exception e){
			assertSame(expectedException, e);
			throw e;
		}finally{
			verify(datasetProvider);
		}
	}
	
	@Test
	public void beginStartsReadWriteTransaction() throws Exception {
		Dataset dataset = createStrictMock(Dataset.class);
		dataset.begin(ReadWrite.WRITE);
		replay(dataset);
		
		datasetProvider = createNiceMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andReturn(dataset);
		replay(datasetProvider);
		
		new TdbEntityDatabase(id, datasetProvider).begin();
		verify(dataset);
	}
	
	@Test
	public void commitCommitsAndEndsTransaction() throws Exception {
		Dataset dataset = createStrictMock(Dataset.class);
		expect(dataset.isInTransaction()).andReturn(true);
		dataset.commit();
		dataset.end();
		replay(dataset);
		
		datasetProvider = createNiceMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andReturn(dataset);
		replay(datasetProvider);
		
		new TdbEntityDatabase(id, datasetProvider).commit();
		verify(dataset);
	}
	
	@Test
	public void abortAbortsAndEndsTransaction() throws Exception {
		Dataset dataset = createStrictMock(Dataset.class);
		expect(dataset.isInTransaction()).andReturn(true);
		dataset.abort();
		dataset.end();
		replay(dataset);
		
		datasetProvider = createNiceMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andReturn(dataset);
		replay(datasetProvider);
		
		new TdbEntityDatabase(id, datasetProvider).abort();
		verify(dataset);
	}
	
	@Test
	public void safeToCallCommitWhenNotInTransaction() throws EntityDatabaseException{
		new TdbEntityDatabase(id, datasetProvider).commit();
	}
	
	@Test
	public void safeToCallAbortWhenNotInTransaction() throws EntityDatabaseException{
		new TdbEntityDatabase(id, datasetProvider).abort();
	}
}
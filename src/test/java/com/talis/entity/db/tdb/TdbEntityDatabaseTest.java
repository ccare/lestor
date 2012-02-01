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

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.sys.TDBMaker;
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
	public void getExecutesInsideReadTransaction() throws Exception {
		DatasetGraph dsg = TDBFactory.createDatasetGraph(Location.mem());
				
		Dataset dataset = createStrictMock(Dataset.class);
		dataset.begin(ReadWrite.READ);
		expect(dataset.asDatasetGraph()).andReturn(dsg);
		dataset.end();
		replay(dataset);
		
		datasetProvider = createNiceMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andReturn(dataset);
		replay(datasetProvider);
		
		new TdbEntityDatabase(id, datasetProvider).get(subject);
		verify(dataset);
	}
	
	@Test
	public void getAbortsTransactionWhenError() throws Exception {
		DatasetGraph dsg = createStrictMock(DatasetGraph.class);
		dsg.find(Node.ANY, subject, Node.ANY, Node.ANY);
		expectLastCall().andThrow(new RuntimeException("BANG!"));
		replay(dsg);		
		
		Dataset dataset = createStrictMock(Dataset.class);
		dataset.begin(ReadWrite.READ);
		expect(dataset.asDatasetGraph()).andReturn(dsg);
		dataset.abort();
		dataset.end();
		replay(dataset);
		
		datasetProvider = createNiceMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andReturn(dataset);
		replay(datasetProvider);
		
		new TdbEntityDatabase(id, datasetProvider).get(subject);
		verify(dataset, dsg);
	}
	
	@Test
	public void putExecutesInsideWriteTransaction() throws Exception {
		DatasetGraph dsg = TDBFactory.createDatasetGraph(Location.mem());
				
		Dataset dataset = createStrictMock(Dataset.class);
		dataset.begin(ReadWrite.WRITE);
		expect(dataset.asDatasetGraph()).andReturn(dsg);
		dataset.commit();
		dataset.end();
		replay(dataset);
		
		datasetProvider = createNiceMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andReturn(dataset);
		replay(datasetProvider);
		
		new TdbEntityDatabase(id, datasetProvider).put(subject, graph, quads);
		verify(dataset);
	}
	
	@Test
	public void putAbortsTransactionWhenError() throws Exception {
		DatasetGraph dsg = createStrictMock(DatasetGraph.class);
		dsg.getGraph(graph);
		expectLastCall().andThrow(new RuntimeException("BANG!"));
		replay(dsg);		
		
		Dataset dataset = createStrictMock(Dataset.class);
		dataset.begin(ReadWrite.WRITE);
		expect(dataset.asDatasetGraph()).andReturn(dsg);
		dataset.abort();
		dataset.end();
		replay(dataset);
		
		datasetProvider = createNiceMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andReturn(dataset);
		replay(datasetProvider);
		
		new TdbEntityDatabase(id, datasetProvider).put(subject, graph, quads);
		verify(dataset);
	}
	
	@Test
	public void deleteExecutesInsideWriteTransaction() throws Exception {
		DatasetGraph dsg = TDBFactory.createDatasetGraph(Location.mem());
				
		Dataset dataset = createStrictMock(Dataset.class);
		dataset.begin(ReadWrite.WRITE);
		expect(dataset.asDatasetGraph()).andReturn(dsg);
		dataset.commit();
		dataset.end();
		replay(dataset);
		
		datasetProvider = createNiceMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andReturn(dataset);
		replay(datasetProvider);
		
		new TdbEntityDatabase(id, datasetProvider).delete(subject, graph);
		verify(dataset);
	}
	
	@Test
	public void deleteAbortsTransactionWhenError() throws Exception {
		DatasetGraph dsg = createStrictMock(DatasetGraph.class);
		dsg.getGraph(graph);
		expectLastCall().andThrow(new RuntimeException("BANG!"));
		replay(dsg);		
		
		Dataset dataset = createStrictMock(Dataset.class);
		dataset.begin(ReadWrite.WRITE);
		expect(dataset.asDatasetGraph()).andReturn(dsg);
		dataset.abort();
		dataset.end();
		replay(dataset);
		
		datasetProvider = createNiceMock(DatasetProvider.class);
		expect(datasetProvider.getDataset(id)).andReturn(dataset);
		replay(datasetProvider);
		
		new TdbEntityDatabase(id, datasetProvider).delete(subject, graph);
		verify(dataset);
	}
	
	@Test
	public void safeToCallCommitWhenNotInTransaction() throws EntityDatabaseException{
		new TdbEntityDatabase(id, datasetProvider);
	}
	
	@Test
	public void safeToCallAbortWhenNotInTransaction() throws EntityDatabaseException{
		new TdbEntityDatabase(id, datasetProvider);
	}
}

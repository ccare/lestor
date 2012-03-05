package com.talis.entity.db.babudb.bulk;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xtreemfs.babudb.BabuDBFactory;
import org.xtreemfs.babudb.api.BabuDB;
import org.xtreemfs.babudb.api.database.Database;
import org.xtreemfs.babudb.api.database.ResultSet;
import org.xtreemfs.babudb.api.exception.BabuDBException;
import org.xtreemfs.babudb.config.BabuDBConfig;
import org.xtreemfs.babudb.config.ConfigBuilder;
import org.xtreemfs.babudb.index.DefaultByteRangeComparator;
import org.xtreemfs.babudb.index.writer.DiskIndexWriter;
import org.xtreemfs.babudb.log.DiskLogger.SyncMode;

import com.google.common.collect.Iterables;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.talis.entity.compress.SnappyCodec;
import com.talis.entity.db.babudb.BabuDBFactoryWrapper;
import com.talis.entity.db.babudb.BabuDbEntityDatabase;
import com.talis.entity.db.babudb.DatabaseManager;
import com.talis.entity.marshal.Marshaller;
import com.talis.sort.ExternalSortIterator;
import com.talis.sort.ExternalSortWriter;
import com.talis.sort.PassThruSerializer;

@SuppressWarnings("PMD")
public class BabuDbEntityDatabaseBuilder {

	private static final Logger LOG = LoggerFactory.getLogger(BabuDbEntityDatabaseBuilder.class);
	
	/**
	 * Build a BabuDB database for use as a EntityDatabase
	 * @param quads A stream of the input quads - must be sorted by subject, then by graph
	 * @param outputDir The output location of the BabuDB files
	 * @throws IOException 
	 * @throws BabuDBException 
	 */
	public void build(InputStream quads, File tmpDir, File outputDir, String dbName) throws IOException, BabuDBException{
		LOG.info("Building entity database in {}", outputDir.getAbsolutePath());
		File dbDir = new File(outputDir, dbName);

		initWorkingDirs(tmpDir);
		File tmpDbDir = new File(tmpDir, "db");
		File tmpSortDir = new File(tmpDir, "sort");
		FileUtils.forceMkdir(tmpSortDir);
		Comparator<byte[]> comparator = new DefaultByteRangeComparator();
		PassThruSerializer serializer = new PassThruSerializer();
		
		ExternalSortWriter<byte[]> sortWriter = new ExternalSortWriter<byte[]>(tmpSortDir, serializer, comparator, 100000, 2, true); 
		
		EntityIterator entities = new EntityIterator(quads, new Marshaller( new SnappyCodec() ) );
		ResultSet<Object, Object> iterator = new EntityResultSet(entities ,sortWriter);
		
		// values from default babudb config
		int maxNumRecordsPerBlock = 64;
		int maxBlockFileSize = 52428800;
		boolean compressIndex = false;
		
		// Write the first babudb index (subject:graph -> po), by virtue of the
		// KeySortWriter, this has the side effect of writing the tmp files to 
		// be used by the merge sort that provides an iterator to build the inverse 
		// (graph:subject -> subject:graph) index in the next step
		DiskIndexWriter firstWriter = new DiskIndexWriter(tmpDbDir.getAbsolutePath(), maxNumRecordsPerBlock, compressIndex, maxBlockFileSize);
		firstWriter.writeIndex(iterator);
		sortWriter.flush();
		copyIndexFiles(tmpDbDir, dbDir, "IX0V0SEQ0.idx");
		sortWriter.waitForCompletion();
		FileUtils.deleteDirectory(tmpDbDir);
		
		// Use a new writer, as they keep an index of the block files they've written,
		// and we want to start again at 0 for the second set of index files
		InverseIndexResultSet inverseIterator = new InverseIndexResultSet(
				new ExternalSortIterator<byte[]>(sortWriter.getTmpDir(), serializer, comparator, sortWriter.compressed()));
		DiskIndexWriter secondWriter = new DiskIndexWriter(tmpDbDir.getAbsolutePath(), maxNumRecordsPerBlock, compressIndex, maxBlockFileSize);
		secondWriter.writeIndex(inverseIterator);
		copyIndexFiles(tmpDbDir, dbDir, "IX1V0SEQ0.idx");
		
		BabuDB dbSystem = makeDatabaseSystem(outputDir);
		Database db = dbSystem.getDatabaseManager().createDatabase(dbName, 2);
		db.shutdown();
		dbSystem.shutdown();
		LOG.info("Done building entity database");
	}
	
	private BabuDB makeDatabaseSystem(File rootDir){
		BabuDBConfig config = new ConfigBuilder()
									.setCompressed(false)
									.setMultiThreaded(0)
									.setLogAppendSyncMode(SyncMode.ASYNC)
									.build();
		// now set our specific properties
		Properties props = config.getProps();
		props.setProperty("babudb.baseDir", rootDir.getAbsolutePath());
		props.setProperty("babudb.logDir", rootDir.getAbsolutePath() + "/logs");
		props.setProperty("babudb.checkInterval", "30");
		props.setProperty("babudb.maxLogfileSize", "" + 1024 * 1024 * 256);
		props.setProperty("babudb.debug.level", "" + 6);
		
		try {
			BabuDBConfig conf = new BabuDBConfig(props);
			return BabuDBFactory.createBabuDB(conf);
		} catch (Exception e) {
			LOG.error("Unable to initialise Database system", e);
			throw new RuntimeException("Error initialising Database system", e);
		}
	}
	
	private void copyIndexFiles(File source, File target, String indexName) throws IOException{
		FileUtils.copyDirectory(source, new File(target, indexName));
	}
	
	private void initWorkingDirs(File rootDir) throws IOException{
       	LOG.info("Initialising Database root directory: {}", rootDir.getAbsolutePath());
       	if (!rootDir.isDirectory() && rootDir.exists()) {
			String msg = String.format("Invalid Database root: {}", rootDir.getAbsolutePath());
			LOG.error(msg);
			throw new RuntimeException(msg);
   		}
       	FileUtils.forceMkdir(rootDir);
       	FileUtils.cleanDirectory(rootDir);
	}
	
	
	public static void main(String[] args) throws Exception{
		BabuDbEntityDatabaseBuilder builder = new BabuDbEntityDatabaseBuilder();
		File out = new File("/tmp/babudb/out");
		File tmp  = new File("/tmp/babudb/tmp");
		builder.build(new FileInputStream(new File("/tmp/quads/quads.nq")), tmp, out, "db");
		DatabaseManager dbm = new DatabaseManager(out, new BabuDBFactoryWrapper());
		BabuDbEntityDatabase edb = new BabuDbEntityDatabase(new Marshaller(new SnappyCodec()), "db", dbm);
		for (Entry<Node, Iterable<Quad>> entity : edb.all()){
			if (Iterables.size(entity.getValue()) == 0){
				System.out.println(entity.getKey().getURI());
			}
		}
		System.out.println("Done");
	}
}

package com.g414.inno.db;

import static org.testng.Assert.assertTrue;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import org.testng.annotations.Test;

import com.g414.inno.db.tpl.DatabaseTemplate;
import com.g414.inno.db.tpl.TransactionCallback;

@Test
public class G414InnoDBTest {
	private Database db = new Database();
	private DatabaseTemplate dt = new DatabaseTemplate(db);

	public void testInno() throws Exception {
		try {
			System.out.println("test inno");
			db.createDatabase(G414InnoDBTableDefs.SCHEMA_NAME);
			db.createTable(G414InnoDBTableDefs.TABLE_1);

			readRows(dt);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			db.dropDatabase(G414InnoDBTableDefs.SCHEMA_NAME);
		}
	}

	private void readRows(final DatabaseTemplate d) throws Exception {
		d.inTransaction(TransactionLevel.REPEATABLE_READ,
				new TransactionCallback<Object>() {
					public Object inTransaction(Transaction txn) {
						System.out.println("inserting rows...");
						Random random = new Random();
						byte[] rand = new byte[16384];

						for (int i = 0; i < 10000; i++) {
							int j = 5000 - i;
							random.nextBytes(rand);

							Map<String, Object> data = new LinkedHashMap<String, Object>();

							data.put("c1", "hello" + i + " "
									+ System.currentTimeMillis());
							data.put("c2", "world" + j + " "
									+ System.currentTimeMillis() + 17);
							data.put("c3", Long.valueOf(j));
							data.put("c4", System.nanoTime()
									* (1 + ((i % 2) * -2)));
							data.put("c5", i % 2 == 1 ? null : Byte
									.valueOf((byte) 1));
							data.put("c6", rand);

							d.insert(txn, G414InnoDBTableDefs.TABLE_1, data);

							if (i % 1000 == 0) {
								System.out.println(new Date() + " make row "
										+ i);
							}
						}

						System.out.println(new Date() + " done.");
						return null;
					}
				});

		dt.inTransaction(TransactionLevel.REPEATABLE_READ,
				new TransactionCallback<Object>() {
					@Override
					public Object inTransaction(Transaction txn) {
						System.out.println("traversing rows...");

						Cursor c = txn.openTable(G414InnoDBTableDefs.TABLE_1);
						Tuple tupl = c.createClusteredIndexReadTuple();

						try {
							c.last();

							int i = 0;
							while (c.hasNext()) {
								c.readRow(tupl);
								Map<String, Object> row = tupl.valueMap();

								assertTrue(((String) row.get("c1"))
										.startsWith("hello"));
								assertTrue(((String) row.get("c2"))
										.startsWith("world"));
								assertTrue(((Number) row.get("c3")) != null);

								c.prev();

								tupl.clear();
								i += 1;
							}
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							tupl.delete();
							c.close();
						}

						return null;
					}
				});

		dt.inTransaction(TransactionLevel.REPEATABLE_READ,
				new TransactionCallback<Object>() {
					@Override
					public Object inTransaction(Transaction txn) {
						System.out.println("traversing secondary index...");

						Cursor c = txn.openTable(G414InnoDBTableDefs.TABLE_1);
						Cursor sec = c.openIndex("c3");
						sec.last();

						int i = 0;
						System.out.println(new Date() + " read sec...");
						Tuple tupl = sec.createSecondaryIndexReadTuple();

						while (sec.hasNext()) {
							sec.readRow(tupl);

							Map<String, Object> row = tupl.valueMap();

							assertTrue(((String) row.get("c1"))
									.startsWith("hello"));
							assertTrue(((String) row.get("c2"))
									.startsWith("world"));
							assertTrue(((Number) row.get("c3")) != null);

							sec.prev();

							tupl.clear();
							i += 1;
						}
						System.out.println(new Date() + " read " + i);
						System.out.println(new Date() + " done.");

						tupl.delete();

						sec.close();
						c.close();

						return null;
					}
				});

		dt.inTransaction(TransactionLevel.REPEATABLE_READ,
				new TransactionCallback<Object>() {
					@Override
					public Object inTransaction(Transaction txn) {
						System.out.println("deleting rows...");

						Cursor c = txn.openTable(G414InnoDBTableDefs.TABLE_1);
						Tuple tupl = c.createClusteredIndexReadTuple();

						try {
							c.last();

							int i = 0;
							while (c.hasNext()) {
								c.readRow(tupl);
								c.deleteRow();
								c.prev();

								tupl.clear();
								i += 1;
							}
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							tupl.delete();
							c.close();
						}

						return null;
					}
				});
	}
}

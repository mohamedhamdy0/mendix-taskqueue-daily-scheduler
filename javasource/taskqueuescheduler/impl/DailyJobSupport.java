package taskqueuescheduler.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import com.mendix.core.Core;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;

/**
 * Cluster-safe helpers for daily task-queue jobs (used by JA_ScheduleDailyMicroflow and
 * JA_ClaimDailyRun).
 *
 * A "claim" is one row in table mx_daily_job_claim with primary key (job_key, slot). Inserting
 * the row is atomic in the database, so when several app instances try the same claim at the
 * same time exactly one succeeds. The table is created automatically on first use; it is not part
 * of the Mendix domain model (Mendix ignores tables it does not own).
 *
 * Do not call these helpers from inside a long transaction that has written data (for example
 * directly in the after-startup microflow): on HSQLDB the one-time CREATE TABLE waits for open
 * transactions. Start them from a task-queue task instead (queue your scheduling microflow from the
 * after-startup microflow).
 */
public final class DailyJobSupport {

	public static final ILogNode LOG = Core.getLogger("TaskQueueScheduler");
	static final String TABLE = "mx_daily_job_claim";
	static final int KEEP_DAYS = 120;
	/** Never wait longer than this for the database (e.g. a lock held by another transaction). */
	static final int SQL_TIMEOUT_SECONDS = 20;

	private DailyJobSupport() {
	}

	/** True if this caller won the claim (key, slot); false if another caller already holds it. */
	public static boolean claim(String key, String slot) {
		return Core.dataStorage().executeWithConnection(conn -> {
			try {
				ensureTable(conn);
				final boolean autoCommit = conn.getAutoCommit();
				try (PreparedStatement ps = conn.prepareStatement(
						"INSERT INTO " + TABLE + " (job_key, slot, claimed_at) VALUES (?, ?, ?)")) {
					ps.setQueryTimeout(SQL_TIMEOUT_SECONDS);
					ps.setString(1, key);
					ps.setString(2, slot);
					ps.setString(3, Instant.now().toString());
					ps.executeUpdate();
					if (!autoCommit) {
						conn.commit();
					}
					return true;
				} catch (SQLException e) {
					if (!autoCommit) {
						conn.rollback();
					}
					if (isDuplicateKey(e)) {
						return false;
					}
					throw e;
				}
			} catch (SQLException e) {
				throw new RuntimeException("Daily job claim failed for " + key + " / " + slot + ": " + e.getMessage(), e);
			}
		});
	}

	/** Best effort: removes claims of this key older than KEEP_DAYS. Errors are only logged. */
	public static void cleanup(String key, String oldestSlotToKeep) {
		try {
			Core.dataStorage().executeWithConnection(conn -> {
				try {
					final boolean autoCommit = conn.getAutoCommit();
					try (PreparedStatement ps = conn.prepareStatement(
							"DELETE FROM " + TABLE + " WHERE job_key = ? AND slot < ?")) {
						ps.setQueryTimeout(SQL_TIMEOUT_SECONDS);
						ps.setString(1, key);
						ps.setString(2, oldestSlotToKeep);
						ps.executeUpdate();
					}
					if (!autoCommit) {
						conn.commit();
					}
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
				return null;
			});
		} catch (RuntimeException e) {
			LOG.warn("Cleanup of old daily job claims failed (ignored): " + e.getMessage());
		}
	}

	/** First waiting (Idle/Retrying) execution of the microflow planned in the future, or null. */
	public static IMendixObject futurePendingTask(String microflowName) {
		final List<IMendixObject> found = Core
				.createXPathQuery("//System.QueuedTask[MicroflowName=$mf][Status='Idle' or Status='Retrying']"
						+ "[StartAt > '[%CurrentDateTime%]']")
				.setVariable("mf", microflowName)
				.setAmount(1)
				.addSort("StartAt", true)
				.execute(Core.createSystemContext());
		return found.isEmpty() ? null : found.get(0);
	}

	public static Date startAtOf(IMendixObject task) {
		return task.getValue(Core.createSystemContext(), "StartAt");
	}

	/** Enqueues the microflow in its own, immediately committed transaction. */
	public static void enqueueNow(String microflowName, String queueName, Date startAt) {
		final IContext ctx = Core.createSystemContext();
		ctx.startTransaction();
		try {
			Core.microflowCall(microflowName).executeInBackground(ctx, queueName, startAt);
			ctx.endTransaction();
		} catch (RuntimeException e) {
			ctx.rollbackTransaction();
			throw e;
		}
	}

	private static void ensureTable(Connection conn) throws SQLException {
		if (tableExists(conn)) {
			return;
		}
		final boolean autoCommit = conn.getAutoCommit();
		try (Statement st = conn.createStatement()) {
			st.setQueryTimeout(SQL_TIMEOUT_SECONDS);
			st.executeUpdate("CREATE TABLE " + TABLE + " (job_key VARCHAR(200) NOT NULL, slot VARCHAR(40) NOT NULL, "
					+ "claimed_at VARCHAR(40), PRIMARY KEY (job_key, slot))");
			if (!autoCommit) {
				conn.commit();
			}
			LOG.info("Created table " + TABLE + " for daily job claims.");
		} catch (SQLException e) {
			if (!autoCommit) {
				conn.rollback();
			}
			// Another instance may have created it at the same moment.
			if (!tableExists(conn)) {
				throw e;
			}
		}
	}

	private static boolean tableExists(Connection conn) throws SQLException {
		final DatabaseMetaData md = conn.getMetaData();
		for (String name : new String[] { TABLE, TABLE.toUpperCase() }) {
			try (ResultSet rs = md.getTables(null, null, name, new String[] { "TABLE" })) {
				if (rs.next()) {
					return true;
				}
			}
		}
		return false;
	}

	private static boolean isDuplicateKey(SQLException e) {
		for (Throwable t = e; t != null; t = t.getCause()) {
			if (t instanceof SQLException) {
				final SQLException s = (SQLException) t;
				final String state = s.getSQLState();
				if ((state != null && state.startsWith("23")) || s.getErrorCode() == 2627 || s.getErrorCode() == 2601
						|| s.getErrorCode() == 1) {
					return true;
				}
			}
		}
		return false;
	}
}

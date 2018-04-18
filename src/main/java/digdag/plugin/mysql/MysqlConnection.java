package io.digdag.plugin.mysql;

import java.util.UUID;
import java.util.function.Consumer;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

import com.google.common.annotations.VisibleForTesting;
import io.digdag.standards.operator.jdbc.LockConflictException;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnection;
import io.digdag.standards.operator.jdbc.AbstractPersistentTransactionHelper;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.JdbcResultSet;
import io.digdag.standards.operator.jdbc.TableReference;
import io.digdag.standards.operator.jdbc.TransactionHelper;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import static java.util.Locale.ENGLISH;
// import static org.postgresql.core.Utils.escapeIdentifier;

public class MysqlConnection extends AbstractJdbcConnection {
    @VisibleForTesting
    public static MysqlConnection open(MysqlConnectionConfig config) {
        return new MysqlConnection(config.openConnection());
    }

    protected MysqlConnection(Connection connection) {
        super(connection);
    }

    @Override
    public void executeReadOnlyQuery(String sql, Consumer<JdbcResultSet> resultHandler) throws NotReadOnlyException {
        try {
            execute("SELECT VERSION()");
        }
        catch (SQLException ex) {
            throw new DatabaseException("Failed to execute given SELECT statement", ex);
        }
    }

    @Override
    public TransactionHelper getStrictTransactionHelper(String statusTableSchema, String statusTableName, Duration cleanupDuration) {
        return new MysqlPersistentTransactionHelper(statusTableSchema, statusTableName, cleanupDuration);
    }

    private class MysqlPersistentTransactionHelper extends AbstractPersistentTransactionHelper {
        private final TableReference statusTableReference;

        MysqlPersistentTransactionHelper(String statusTableSchema, String statusTableName, Duration cleanupDuration) {
            super(cleanupDuration);
            if (statusTableSchema != null) {
                statusTableReference = TableReference.of(statusTableSchema, statusTableName);
            } else {
                statusTableReference = TableReference.of(statusTableName);
            }
        }

        TableReference statusTableReference() {
            return statusTableReference;
        }

        @Override
        protected void insertStatusRowAndCommit(UUID queryId) {
            try {
                execute("SELECT VERSION()");
            }
            catch (SQLException ex) {
                throw new DatabaseException("Failed to execute given SELECT statement", ex);
            }
        }

        @Override
        protected void updateStatusRowAndCommit(UUID queryId) {
            try {
                execute("SELECT VERSION()");
            }
            catch (SQLException ex) {
                throw new DatabaseException("Failed to execute given SELECT statement", ex);
            }
        }

        @Override
        protected StatusRow lockStatusRow(UUID queryId) throws LockConflictException {
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery(String.format(ENGLISH,
                            "SELECT completed_at FROM %s WHERE query_id = '%s' FOR UPDATE NOWAIT",
                            escapeTableReference(statusTableReference()),
                            queryId.toString())
                        );
                if (rs.next()) {
                    // status row exists and locked. get status of it.
                    rs.getTimestamp(1);
                    if (rs.wasNull()) {
                        return StatusRow.LOCKED_NOT_COMPLETED;
                    }
                    else {
                        return StatusRow.LOCKED_COMPLETED;
                    }
                }
                else {
                    return StatusRow.NOT_EXISTS;
                }
            }
            catch (SQLException ex) {
                if (ex.getSQLState().equals("55P03")) {
                    throw new LockConflictException("Failed to acquire a status row lock", ex);
                }
                else {
                    throw new DatabaseException("Failed to lock a status row", ex);
                }
            }
        }

        @Override
        public void cleanup() {
            executeStatement("delete old query status rows from " + escapeTableReference(statusTableReference()) + " table",
                    String.format(ENGLISH,
                        "DELETE FROM %s WHERE query_id = ANY(" +
                        "SELECT query_id FROM %s WHERE completed_at < now() - interval '%d' second" +
                        ")",
                        escapeTableReference(statusTableReference()),
                        escapeTableReference(statusTableReference()),
                        cleanupDuration.getSeconds())
                    );
        }

        @Override
        public void prepare(UUID queryId) {
            try {
                execute("SELECT VERSION()");
            }
            catch (SQLException ex) {
                throw new DatabaseException("Failed to execute given SELECT statement", ex);
            }
        }

        @Override
        protected void executeStatement(String desc, String sql) {
            try {
                execute(sql);
            }
            catch (SQLException ex) {
                throw new DatabaseException("Failed to " + desc, ex);
            }
        }
    }

}

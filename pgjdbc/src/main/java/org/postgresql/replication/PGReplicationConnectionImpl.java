/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.replication;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.QueryExecutor;
import org.postgresql.replication.fluent.ChainedCreateReplicationSlotBuilder;
import org.postgresql.replication.fluent.ChainedStreamBuilder;
import org.postgresql.replication.fluent.ReplicationCreateSlotBuilder;
import org.postgresql.replication.fluent.ReplicationStreamBuilder;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;

public class PGReplicationConnectionImpl implements PGReplicationConnection {
  private BaseConnection connection;

  public PGReplicationConnectionImpl(BaseConnection connection) {
    this.connection = connection;
  }

  @Override
  public ChainedStreamBuilder replicationStream() {
    return new ReplicationStreamBuilder(connection);
  }

  @Override
  public ChainedCreateReplicationSlotBuilder createReplicationSlot() {
    return new ReplicationCreateSlotBuilder(connection);
  }

  @Override
  public void dropReplicationSlot(String slotName) throws SQLException {
    if (slotName == null || slotName.isEmpty()) {
      throw new IllegalArgumentException("Replication slot name can't be null or empty");
    }

    BaseStatement stmt = (BaseStatement) connection.createStatement();
    try {
      if (stmt.executeWithFlags("DROP_REPLICATION_SLOT " + slotName, QueryExecutor.QUERY_ONESHOT
          | QueryExecutor.QUERY_EXECUTE_AS_SIMPLE | QueryExecutor.QUERY_NO_METADATA
          | QueryExecutor.QUERY_NO_RESULTS | QueryExecutor.QUERY_SUPPRESS_BEGIN)) {
        throw new PSQLException(GT.tr("A result was returned when none was expected."),
            PSQLState.TOO_MANY_RESULTS);
      }
    } finally {
      stmt.close();
    }
  }
}

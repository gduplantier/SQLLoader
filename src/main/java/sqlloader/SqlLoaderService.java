package sqlloader;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import java.sql.*;
import java.util.ArrayList;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SqlLoaderService {

  private Logger logger = LoggerFactory.getLogger(SqlLoaderService.class);

  @Value("${jdbc.driver.url}")
  private String dbUrl;

  @Value("${jdbc.driver.username}")
  private String username;

  @Value("${jdbc.driver.password}")
  private String password;

  @Value("${sqlloader.source}")
  private String sourceName;

  @Value("${sqlloader.schema}")
  private String schemaName;

  @Value("${sqlloader.table}")
  private String tableName;

  @Value("${sqlloader.metadata}")
  private String metadata;

  @Value("${marklogic.collections}")
  private String collections;

  final DatabaseClient marklogic;
  final DataMovementManager manager;
  final WriteBatcher mlWriter;
  final JobTicket ticket;

  public SqlLoaderService(
    @Value("${marklogic.host}") String mlHost,
    @Value("${marklogic.port}") int mlPort,
    @Value("${marklogic.username}") String mlUsername,
    @Value("${marklogic.password}") String mlPassword,
    @Value("${marklogic.ssl.enabled}") boolean sslEnabled,
    @Value("${marklogic.ssl.verifyhostname}") boolean verifyHostname,
    @Value("${marklogic.auth.scheme}") String authScheme
  ) {
    DatabaseClientFactory.SecurityContext securityContext = null;

    // configure authentication scheme, digest or basic
    if (authScheme.equals("digest")) {
      securityContext =
        new DatabaseClientFactory.DigestAuthContext(mlUsername, mlPassword);
    } else {
      securityContext =
        new DatabaseClientFactory.BasicAuthContext(mlUsername, mlPassword);
    }

    // Configure SSL/TLS if enabled
    // Configure verify hostname. Usually false for self signed certs in Dev
    // environment.
    if (sslEnabled) {
      SSLContext ctx = null;
      try {
        ctx = SSLContext.getInstance("TLSv1.2"); // Minimum version for FIPS compliance.
        ctx.init(null, null, null); // use default Java key/trust store in JAVA_HOME dir
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }

      if (verifyHostname) {
        securityContext
          .withSSLContext(ctx, null)
          .withSSLHostnameVerifier(
            DatabaseClientFactory.SSLHostnameVerifier.COMMON
          );
      } else {
        securityContext
          .withSSLContext(ctx, null)
          .withSSLHostnameVerifier(
            DatabaseClientFactory.SSLHostnameVerifier.ANY
          );
      }
    }

    marklogic =
      DatabaseClientFactory.newClient(mlHost, mlPort, securityContext);

    manager = marklogic.newDataMovementManager();

    // Need to test running with thread count > 1. Not sure if code is thread safe.
    mlWriter =
      manager
        .newWriteBatcher()
        .withJobName("SQLLoader")
        .withBatchSize(100)
        .withThreadCount(1)
        .onBatchSuccess(batch -> {
          logger.info("Batch Success");
        })
        .onBatchFailure((batch, throwable) -> {
          logger.info("Batch Failure: {}", throwable.getMessage());
        });

    ticket = manager.startJob(mlWriter);
  }

  public void load() {
    logger.info(
      "--------------- Loading data for table: {} ---------------------",
      tableName
    );

    try (
      Connection conn = DriverManager.getConnection(dbUrl, username, password);
      Statement stmt = conn.createStatement();
      Statement stmt2 = conn.createStatement();
      ResultSet rs2 = stmt2.executeQuery(
        "SELECT column_name FROM all_cons_columns WHERE owner = UPPER('" +
        schemaName +
        "') AND constraint_name = (SELECT constraint_name FROM all_constraints WHERE UPPER(table_name) = UPPER('" +
        tableName +
        "') AND OWNER = UPPER('" +
        schemaName +
        "') AND CONSTRAINT_TYPE = 'P')"
      );
      ResultSet rs = stmt.executeQuery(
        "select * from " + schemaName + "." + tableName
      )
    ) {
      ArrayList<String> primaryKeys = queryPrimaryKeys(rs2);

      // Oracle default is 10.
      rs.setFetchSize(100);

      while (rs.next()) {
        String uriIdVal = buildUriId(rs, primaryKeys);
        String uri =
          "/" +
          sourceName +
          "/" +
          schemaName +
          "/" +
          tableName +
          "/" +
          uriIdVal +
          ".xml";
        String xmlResult = ResultSetParserUtil.convertRecordToXML(rs, metadata);
        StringHandle stringHandle = new StringHandle(xmlResult)
          .withFormat(Format.XML);
        DocumentMetadataHandle docMetadataHandle = new DocumentMetadataHandle();

        if (collections != null) {
          for (String collection : collections.split(",")) {
            docMetadataHandle.withCollections(collection);
          }
        }

        mlWriter.add(uri, docMetadataHandle, stringHandle);
      }
    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
    } catch (Exception e1) {
      logger.error(e1.getMessage(), e1);
    }
    mlWriter.flushAndWait();
    manager.stopJob(ticket);
  }

  // Get primary key(s) of table to use as unique URI property
  // Will handle composite keys by concatenating them
  private ArrayList<String> queryPrimaryKeys(ResultSet rs) throws SQLException {
    ArrayList<String> primaryKeys = new ArrayList<>();
    while (rs.next()) {
      String colVal = rs.getString(1);
      primaryKeys.add(colVal);
      logger.info("Using value in column: {} for unique URI value", colVal);
    }
    if (primaryKeys.isEmpty()) logger.info(
      "Using a randomly generated UUID for unique URI value"
    );

    return primaryKeys;
  }

  private String buildUriId(ResultSet rs, ArrayList<String> primaryKeys)
    throws SQLException {
    StringBuilder uriIdVal = new StringBuilder();
    if (primaryKeys.isEmpty()) {
      uriIdVal.append(UUID.randomUUID().toString());
    } else {
      int index = 0;
      if (index > 1) uriIdVal.append("-");
      for (String primaryKey : primaryKeys) {
        uriIdVal.append(rs.getObject(primaryKey));
        index++;
      }
    }
    return uriIdVal.toString();
  }
}

package sqlloader;

import java.sql.*;
import java.util.UUID;

import javax.net.ssl.SSLContext;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.JobTicket;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

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
            @Value("${marklogic.auth.scheme}") String authScheme) {
        DatabaseClientFactory.SecurityContext securityContext = null;

        // configure authentication scheme, digest or basic
        if (authScheme.equals("digest")) {
            securityContext = new DatabaseClientFactory.DigestAuthContext(mlUsername, mlPassword);
        } else {
            securityContext = new DatabaseClientFactory.BasicAuthContext(mlUsername, mlPassword);
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
                securityContext.withSSLContext(ctx, null)
                        .withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.COMMON);
            } else {
                securityContext.withSSLContext(ctx, null)
                        .withSSLHostnameVerifier(DatabaseClientFactory.SSLHostnameVerifier.ANY);
            }
        }

        marklogic = DatabaseClientFactory.newClient(mlHost, mlPort, securityContext);

        manager = marklogic.newDataMovementManager();

        // Need to test running with thread count > 1. Not sure if code is thread safe.
        mlWriter = manager
                .newWriteBatcher()
                .withJobName("SQLLoader")
                .withBatchSize(100)
                .withThreadCount(1)
                .onBatchSuccess(batch -> {
                    logger.info("Batch Success");
                })
                .onBatchFailure((batch, throwable) -> {
                    logger.info("Batch Failure: " + throwable.getMessage());
                });

        ticket = manager.startJob(mlWriter);
    }

    public void load() {
        try (Connection conn = DriverManager.getConnection(dbUrl, username, password);
                Statement stmt = conn.createStatement();
                Statement stmt2 = conn.createStatement();
                ResultSet rs2 = stmt2.executeQuery("SELECT column_name FROM all_cons_columns"
                        + " WHERE constraint_name = (SELECT constraint_name FROM all_constraints WHERE UPPER(table_name) = UPPER('"
                        + tableName + "') AND OWNER = UPPER('" + schemaName + "') AND CONSTRAINT_TYPE = 'P')");
                ResultSet rs = stmt.executeQuery("select * from " + schemaName + "." + tableName)) {

            logger.info("--------------- Loading data for table: " + tableName + " ---------------");
            // Get primary key of table to use as unique URI property
            String uriId = null;
            if (rs2.next()) {
                uriId = rs2.getString(1);
                logger.info("Using value in column: " + uriId + " for unique URI value");
            } else {
                logger.info("Using a randomly generated UUID for unique URI value");
            }

            // Oracle default is 10.
            rs.setFetchSize(100);

            while (rs.next()) {
                Object uriIdVal = (uriId == null || uriId.equals("")) ? UUID.randomUUID().toString()
                        : rs.getObject(uriId);
                String uri = "/" + sourceName + "/" + schemaName + "/" + tableName + "/" + uriIdVal + ".xml";
                String xmlResult = ResultSetParserUtil.convertRecordToXML(rs, metadata);
                StringHandle stringHandle = new StringHandle(xmlResult).withFormat(Format.XML);
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
}

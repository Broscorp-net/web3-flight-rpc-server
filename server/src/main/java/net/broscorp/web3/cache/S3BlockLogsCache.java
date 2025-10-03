package net.broscorp.web3.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.web3j.protocol.core.methods.response.Log;

import java.io.File;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class S3BlockLogsCache implements RestorableBlockLogsCache {

    private static final String DB_FILE = "/tmp/blocklogs.sqlite";
    private static final String TABLE = "block_logs";

    private final ObjectMapper mapper = new ObjectMapper();
    private final Connection connection;
    private final S3Client s3;
    private final String bucket;
    private final String fileKey;

    public S3BlockLogsCache(String bucket, String fileKey, String region) throws Exception {
        this.bucket = bucket;
        this.fileKey = fileKey;
        this.s3 = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Download snapshot if exists
        downloadSnapshot();

        // Init SQLite connection
        this.connection = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);
        initSchema();
    }

    private void initSchema() throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (" +
                    "block_number TEXT PRIMARY KEY, " +
                    "logs TEXT NOT NULL, " +
                    "created_at INTEGER NOT NULL" +
                    ")");
        }
    }

    public void downloadSnapshot() {
        Path dbPath = Paths.get(DB_FILE);
        if (Files.exists(dbPath)) {
            System.out.println("Local snapshot already exists at " + DB_FILE + ", skipping S3 download.");
            return;
        }

        try {
            s3.getObject(
                    GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(fileKey)
                            .build(),
                    ResponseTransformer.toFile(dbPath)
            );
            System.out.println("Loaded snapshot from S3: " + fileKey);
        } catch (NoSuchKeyException e) {
            System.out.println("No snapshot found in S3, starting fresh.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void uploadSnapshot() {
        try {
            s3.putObject(
                    PutObjectRequest.builder().bucket(bucket).key(fileKey).build(),
                    RequestBody.fromFile(new File(DB_FILE))
            );
            System.out.println("Uploaded snapshot to S3: " + fileKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        uploadSnapshot();
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void put(BigInteger blockNumber, List<Log> logs) {
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT OR REPLACE INTO " + TABLE + " (block_number, logs, created_at) VALUES (?, ?, ?)"
        )) {
            String json = mapper.writeValueAsString(logs);
            ps.setString(1, blockNumber.toString());
            ps.setString(2, json);
            ps.setLong(3, Instant.now().getEpochSecond());
            ps.executeUpdate();

            cleanup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Log> get(BigInteger blockNumber) {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT logs FROM " + TABLE + " WHERE block_number = ?"
        )) {
            ps.setString(1, blockNumber.toString());
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String json = rs.getString(1);
                return mapper.readValue(json, new TypeReference<List<Log>>() {});
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Override
    public boolean contains(BigInteger blockNumber) {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT 1 FROM " + TABLE + " WHERE block_number = ?"
        )) {
            ps.setString(1, blockNumber.toString());
            return ps.executeQuery().next();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean containsRange(BigInteger startBlock, BigInteger endBlock) {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT COUNT(*) FROM " + TABLE + " WHERE CAST(block_number AS INTEGER) BETWEEN ? AND ?"
        )) {
            ps.setLong(1, startBlock.longValue());
            ps.setLong(2, endBlock.longValue());
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                long count = rs.getLong(1);
                return count == (endBlock.longValue() - startBlock.longValue() + 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private void cleanup() {
        long cutoff = Instant.now().minus(3, ChronoUnit.DAYS).getEpochSecond();

        try (PreparedStatement ps = connection.prepareStatement(
                "DELETE FROM " + TABLE + " WHERE created_at < ?"
        )) {
            ps.setLong(1, cutoff);
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (Statement stmt = connection.createStatement()) {
            // Check if we have more than 10,000 rows
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE);
            if (rs.next() && rs.getInt(1) > 10_000) {
                // Delete oldest 500 rows
                try (PreparedStatement ps = connection.prepareStatement(
                        "DELETE FROM " + TABLE + " WHERE id IN (" +
                                "SELECT id FROM " + TABLE + " ORDER BY created_at ASC LIMIT 500" +
                                ")"
                )) {
                    ps.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

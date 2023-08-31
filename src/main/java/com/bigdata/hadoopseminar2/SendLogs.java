package com.bigdata.hadoopseminar2;

import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;



@Service
public class SendLogs {

    private String errorMessage = "";
    public boolean sendAccessLogs() {

        Process process = null;
        boolean success = false;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.command(
                    "/bin/bash",
                    "-c",
                    "/var/lib/tomcat9/webapps/seminar2/sendAccesslog.sh");

            process = processBuilder.start();
            int exitCode = process.waitFor();

            if (exitCode == 0) {
                success = true;
                System.out.println("Access logs sent successfully.");

            } else {
                success = false;

                System.err.println("Sending access logs failed. Exit code: " + exitCode);
                String errorOutput = readProcessErrorOutput(process);
                System.err.println("Error output: " + errorOutput);

                errorMessage = "Exit code: " + exitCode + errorOutput;
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            errorMessage = e.getMessage();
            System.err.println("Sending access logs failed. Exit code: " + errorMessage);
        }
        return success;
    }
    public String getErrorMessage() {
        return errorMessage;
    }

    private String readProcessErrorOutput(Process process) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
            StringBuilder errorOutput = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                errorOutput.append(line).append(System.lineSeparator());
            }
            return errorOutput.toString();
        }
    }

    public boolean createActivityLogDB() {
        Connection impalaConn = null;
        PreparedStatement pstmt = null;
        boolean success = false;

        try {
            // Impala 연결 정보 설정
            String impalaJdbcUrl = "jdbc:impala:/[dest ip]:21050/activitylog";
            String DbUser = "id";
            String DbPwd = "passwd";

            Class.forName("com.cloudera.impala.jdbc41.Driver");
            impalaConn = DbConnector.getImpalaConnection(impalaJdbcUrl, DbUser, DbPwd);

            // Impala에 DB 생성
            String createActivityLogDB = "CREATE DATABASE IF NOT EXISTS activitylog";
            pstmt = impalaConn.prepareStatement(createActivityLogDB);
            pstmt.executeUpdate();

            success = true;
            System.out.println("Impala에 activitylog Database가 생성되었습니다.");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return success;
    }


    public boolean sendActivityLogs() {
            Connection sourceConn = null;
            Connection impalaConn = null;
            PreparedStatement pstmt = null;
            PreparedStatement pstmt2 = null;
            PreparedStatement pstmt3 = null;
            ResultSet rs = null;
            boolean success = false;

            try {
                // MySQL 연결 정보 설정
                String sourceJdbcUrl = "jdbc:mysql://[source ip]:3306/wordpress";
                String sourceDbUser = "wordpress";
                String sourceDbPwd = "wordpress";

                sourceConn = DbConnector.getMysqlConnection(sourceJdbcUrl, sourceDbUser, sourceDbPwd);

                // 데이터 가져오기
                String fetchDataSQL = "SELECT * FROM wp_ualp_user_activity";
                pstmt = sourceConn.prepareStatement(fetchDataSQL);
                rs = pstmt.executeQuery();

                // Impala 연결 정보 설정
                String impalaJdbcUrl = "jdbc:impala://[dest ip]:21050/activitylog";
                String DbUser = "id";
                String DbPwd = "passwd";

              Class.forName("com.cloudera.impala.jdbc41.Driver");
                impalaConn = DbConnector.getImpalaConnection(impalaJdbcUrl, DbUser, DbPwd);

                // Impala에 External 테이블 생성 및 데이터 삽입
                String createExternalTableSQL = "CREATE EXTERNAL TABLE IF NOT EXISTS wp_ualp_user_activity (" +
                        "uactid INT, " +
                        "post_id INT, " +
                        "post_title STRING, " +
                        "user_id INT, " +
                        "user_name STRING, " +
                        "user_role STRING, " +
                        "user_email STRING, " +
                        "ip_address STRING, " +
                        "modified_date TIMESTAMP, " +
                        "object_type STRING, " +
                        "action STRING) " +
                        "STORED AS PARQUET " +
                        "LOCATION 'hdfs://[hadoop-cluster]/user/hduser/activitylog/'";

                pstmt2 = impalaConn.prepareStatement(createExternalTableSQL);
                pstmt2.executeUpdate();

                String insertSQL = "INSERT INTO wp_ualp_user_activity VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                pstmt3 = impalaConn.prepareStatement(insertSQL);

                while (rs.next()) {
                    pstmt3.setInt(1, rs.getInt("uactid"));
                    pstmt3.setInt(2, rs.getInt("post_id"));
                    pstmt3.setString(3, rs.getString("post_title"));
                    pstmt3.setInt(4, rs.getInt("user_id"));
                    pstmt3.setString(5, rs.getString("user_name"));
                    pstmt3.setString(6, rs.getString("user_role"));
                    pstmt3.setString(7, rs.getString("user_email"));
                    pstmt3.setString(8, rs.getString("ip_address"));
                    pstmt3.setTimestamp(9, rs.getTimestamp("modified_date"));
                    pstmt3.setString(10, rs.getString("object_type"));
                    pstmt3.setString(11, rs.getString("action"));
                    pstmt3.executeUpdate();
                }
                success = true;
                System.out.println("Impala의 wp_ualp_user_activity External 테이블에 데이터가 삽입되었습니다.");
            } catch (SQLException e) {
                e.printStackTrace();
                success = false;
                System.out.println("Impala의 wp_ualp_user_activity External 테이블에 데이터 삽입 실패하였습니다.");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (rs != null) {
                        rs.close();
                    }
                    if (pstmt != null) {
                        pstmt.close();
                    }
                    if (pstmt2 != null) {
                        pstmt2.close();
                    }
                    if (pstmt3 != null) {
                        pstmt3.close();
                    }
                    if (sourceConn != null) {
                        sourceConn.close();
                    }
                    if (impalaConn != null) {
                        impalaConn.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        return success;
        }
    }


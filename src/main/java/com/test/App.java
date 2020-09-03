package com.test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;

/**
 * AWS Lambda function with S3 trigger.
 * 
 */
public class App implements RequestHandler<S3EventNotification, String> {

	static final Logger log = LoggerFactory.getLogger(App.class);
	private String topicArn = "arn:aws:sns:us-east-2:334007224920:s3tosqlstatus";

	@Override
	public String handleRequest(S3EventNotification s3Event, Context context) {
		log.info("Lambda function is invoked:" + s3Event.toJson());
		// AmazonSNSClient snsClient = (AmazonSNSClient)
		// AmazonSNSClientBuilder.standard().build();
		AmazonSNS snsClient = AmazonSNSClientBuilder.standard().withRegion(Regions.US_EAST_2).build();

		byte[] buffer = new byte[1024];
		try {
			if (s3Event.getRecords().size() > 0) {
				S3EventNotificationRecord record = s3Event.getRecords().get(0);
				/* for (S3EventNotificationRecord record : s3Event.getRecords()) { */
				String srcBucket = record.getS3().getBucket().getName();
				String srcBackup = "db-executed-backup";

				// Object key may have spaces or unicode non-ASCII characters.
				String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
				srcKey = URLDecoder.decode(srcKey, "UTF-8");

				// Detect file type
				Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey);
				if (!matcher.matches()) {
					System.out.println("Unable to detect file type for key " + srcKey);
					log.error("Lambda function is invoked:" + s3Event.toJson());
					return "";
				}
				String extension = matcher.group(1).toLowerCase();
				if (!"zip".equals(extension)) {
					log.error("Skipping non-zip file " + srcKey + " with extension " + extension);
					return "";
				}
//				System.out.println("Extracting zip file " + srcBucket + "/" + srcKey);

				// Download the zip from S3 into a stream
				AmazonS3 s3Client = new AmazonS3Client();
				System.out.println("Copy started------------------");
				String desKey = "Arch_" + System.currentTimeMillis() + "_" + srcKey;
				s3Client.copyObject(srcBucket, srcKey, srcBackup, desKey);
				// s3Client.copyObject(srcBucket, srcKey, srcBackup, srcKey);
				System.out.println("Copy ended------------------" + desKey);

				S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBackup, desKey));

				System.out.println("extract from backup------------------" + desKey);
				ZipInputStream zis = new ZipInputStream(s3Object.getObjectContent());

				ZipEntry entry = zis.getNextEntry();
				log.info("unzipping started");
				while (entry != null) {
					String fileName = entry.getName();
					String mimeType = FileMimeType.fromExtension(FilenameUtils.getExtension(fileName)).mimeType();

					ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
					int len;
					while ((len = zis.read(buffer)) > 0) {
						outputStream.write(buffer, 0, len);
					}
					InputStream is = new ByteArrayInputStream(outputStream.toByteArray());

					ObjectMetadata meta = new ObjectMetadata();
					meta.setContentLength(outputStream.size());
					meta.setContentType(mimeType);
					s3Client.putObject(srcBackup, FilenameUtils.getFullPath(srcKey) + fileName, is, meta);
					is.close();
					outputStream.close();
					entry = zis.getNextEntry();

					log.info("unzipped file: " + fileName);
				}
				zis.closeEntry();
				zis.close();
				log.info("unzipping ended and copied to backup");

				String folderName = srcKey.replace(".zip", "");
				String dbConfFileName = folderName + "/Connect2SQLServer.conf";
				String scriptOrderFileName = folderName + "/SQLExecutionOrder.conf";
				System.out.println("Connect2SQLServer----------");

				log.info("reading db config file started");
				List<String> dbConfContent = readFile(s3Client, srcBackup, dbConfFileName);
				log.info("reading db config file ended");

				log.info("reading script order config file started");
				List<String> scriptOrderContent = readFile(s3Client, srcBackup, scriptOrderFileName);
				log.info("reading script order config file ended");

				log.info("connecting to db started");
				connectDB(dbConfContent, scriptOrderContent, s3Client, srcBackup, folderName);
				log.info("connecting to db ended");

				log.info("deleting unzipped folder and files started");
				deleteFile(s3Client, srcBackup, folderName + "/", srcKey);
				log.info("deleting unzipped folder and files ended");

				log.info("deleting zip file started from src bucket");
				s3Client.deleteObject(srcBucket, srcKey);
				log.info("deleting zip file ended from src bucket");

				snsClient.publish(topicArn, "Scripts Exectuation Status is SUCCESS",
						"Scripts Exectuation Status-SUCCESS");
			}
			return "Ok";
		} catch (IOException | ClassNotFoundException | SQLException e) {
			System.out.println("Error occured111");
			snsClient.publish(topicArn, "Scripts Exectuation Status is FAILED", "Scripts Exectuation Status-FAILED");
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	private List<String> readFile(AmazonS3 s3Client, String srcBucket, String dbConfFileName) throws IOException {
		List<String> lines = new ArrayList<String>();
		S3Object s3DBConfObject = s3Client.getObject(new GetObjectRequest(srcBucket, dbConfFileName));
		BufferedReader br = new BufferedReader(new InputStreamReader(s3DBConfObject.getObjectContent(), "UTF-8"));
		String line = br.readLine();
		while (line != null) {
			System.out.println(line);
			lines.add(line);
			line = br.readLine();
		}
		return lines;
	}

	private void deleteFile(AmazonS3 s3Client, String srcBucket, String folderName, String srcKey) {
		for (S3ObjectSummary file : s3Client.listObjects(srcBucket, folderName).getObjectSummaries()) {
			s3Client.deleteObject(srcBucket, file.getKey());
			log.info("Deleted file: " + srcBucket + " " + folderName + "  " + file.getKey());
		}
	}

	private void connectDB(List<String> dbConfContent, List<String> scriptOrderContent, AmazonS3 s3Client,
			String srcBucket, String folderName) throws SQLException, ClassNotFoundException, IOException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection(
				dbConfContent.get(0) + ":" + dbConfContent.get(1) + "/" + dbConfContent.get(2), dbConfContent.get(3),
				dbConfContent.get(4));
		System.out.println(dbConfContent.get(0) + ":" + dbConfContent.get(1) + "/" + dbConfContent.get(2)
				+ dbConfContent.get(3) + dbConfContent.get(4));
		ScriptRunner runner = new ScriptRunner(con, false, false);

		for (int i = 0; i < scriptOrderContent.size(); i++) {
			try {
				S3Object s3DBConfObject = s3Client
						.getObject(new GetObjectRequest(srcBucket, folderName + "/" + scriptOrderContent.get(i)));
				BufferedReader br = new BufferedReader(
						new InputStreamReader(s3DBConfObject.getObjectContent(), "UTF-8"));
				runner.runScript(br);

				insertScriptStatus(con, "PASS", scriptOrderContent.get(i), i + 1);
			} catch (Exception e) {
				insertScriptStatus(con, "FAIL", scriptOrderContent.get(i), i + 1);
			}
			log.info("Executed script: " + scriptOrderContent.get(i));
		}
	}

	public void insertScriptStatus(Connection con, String status, String scriptName, int scriptSeq) {
		String sql = "INSERT INTO dbexecstatusperrel(SerialNo,ScriptName,ExecutionTime,ExecutedBy,ExeStatus) VALUES(?,?,?,?,?)";

		try (PreparedStatement pstmt = con.prepareStatement(sql)) {
			pstmt.setInt(1, scriptSeq);
			pstmt.setString(2, scriptName);
			Calendar calendar = Calendar.getInstance();
			java.util.Date currentTime = calendar.getTime();
			pstmt.setTimestamp(3, new Timestamp(currentTime.getTime()));
			pstmt.setString(4, "DILEEP KUMAR");
			pstmt.setString(5, status);
			pstmt.executeUpdate();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			log.info("Error while adding record to dbexecstatusperrel table : " + e.getMessage());
		}
	}
}
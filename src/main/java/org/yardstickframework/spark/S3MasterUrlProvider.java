/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yardstickframework.spark;

import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.yardstickframework.BenchmarkUtils.*;

/**
 * NOTE: NOT THREAD SAFE!!! Use that class only from one thread.
 */
public class S3MasterUrlProvider {
    /** S3 lock file name. */
    public final String LOCK_KEY = ".lock";

    /** Entry content. */
    private static final byte[] ENTRY_CONTENT = new byte[] {1};

    /** Entry metadata with content length set. */
    private static final ObjectMetadata ENTRY_METADATA;

    static {
        ENTRY_METADATA = new ObjectMetadata();

        ENTRY_METADATA.setContentLength(ENTRY_CONTENT.length);
    }

    /** Client to interact with S3 storage. */
    private AmazonS3 s3;

    /** Bucket name. */
    private String bucketName = "test-spark-bucket";

    /** Flag indicate that client init. */
    private boolean clientInit;

    /** File lock version. */
    private String lockId;

    /**
     * Resolves url to master node.
     *
     * @return Url to master node.
     */
    public String getMasterUrl() {
        initAwsClient();

        String masterUrl = null;

        try {
            ObjectListing list = s3.listObjects(bucketName, SparkMaster.SPARK_URL_PREFIX);

            while (true) {
                for (S3ObjectSummary s3Entry : list.getObjectSummaries())
                    if (s3Entry.getKey() != null && s3Entry.getKey().contains(SparkMaster.SPARK_URL_PREFIX)) {
                        masterUrl = s3KeyToUrl(s3Entry.getKey());

                        break;
                    }

                if (list.isTruncated())
                    list = s3.listNextBatchOfObjects(list);
                else
                    break;
            }
        }
        catch (AmazonClientException e) {
            throw new RuntimeException("Failed to list objects in the bucket: " + bucketName, e);
        }

        return masterUrl;
    }

    public void registerMaster(String masterUrl) {
        initAwsClient();

        try {
            s3.putObject(bucketName, urlToS3Key(masterUrl), new ByteArrayInputStream(ENTRY_CONTENT), ENTRY_METADATA);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed. Couldn't upload master url to s3 bucket. "
                + "Bucket name: [" + bucketName + "], master url: [" + masterUrl + "].", e);
        }
    }

    public void unregisterMaster(String masterUrl) {
        initAwsClient();

        try {
            VersionListing versList = s3.listVersions(bucketName, urlToS3Key(masterUrl));

            for (S3VersionSummary ver : versList.getVersionSummaries())
                s3.deleteVersion(ver.getBucketName(), ver.getKey(), ver.getVersionId());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed. Couldn't unregister master to s3 bucket. "
                + "Bucket name: [" + bucketName + "], master url: [" + masterUrl + "].", e);
        }
    }

    /**
     * @return @{code True} if lock acquired (created .lock file in amazon bucket).
     */
    public boolean tryLock() {
        initAwsClient();

        try {
            VersionListing versList = s3.listVersions(bucketName, LOCK_KEY);

            if (!versList.getVersionSummaries().isEmpty())
                // Lock file exist.
                return false;

            PutObjectResult opResult = s3.putObject(bucketName, LOCK_KEY,
                new ByteArrayInputStream(ENTRY_CONTENT), ENTRY_METADATA);

            List<S3VersionSummary> vers = s3.listVersions(bucketName, LOCK_KEY).getVersionSummaries();

            // We are the first.
            if (opResult.getVersionId().equals(vers.get(vers.size() - 1).getVersionId())) {
                lockId = opResult.getVersionId();

                return true;
            }
            else
                return false;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed. Couldn't acquire lock.", e);
        }
    }

    public void unLock() {
        if (lockId == null)
            return;

        initAwsClient();

        try {
            VersionListing versList = s3.listVersions(bucketName, LOCK_KEY);

            for (S3VersionSummary ver : versList.getVersionSummaries())
                s3.deleteVersion(ver.getBucketName(), ver.getKey(), ver.getVersionId());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed. Couldn't acquire lock.", e);
        }
    }

    /**
     * Method init amazon s3 API.
     */
    private void initAwsClient() {
        if (clientInit)
            return;

        String awsAccessKey = System.getenv("AWS_ACCESS_KEY");

        String awsSecretKey = System.getenv("AWS_SECRET_KEY");

        AWSCredentials cred;

        if (awsAccessKey == null || awsAccessKey.isEmpty() || awsSecretKey == null || awsSecretKey.isEmpty())
            throw new IllegalArgumentException("AWS credentials are not set.");
        else
            cred = new BasicAWSCredentials(awsAccessKey, awsSecretKey);

        if (bucketName == null || bucketName.isEmpty())
            throw new IllegalArgumentException("Bucket name is null or empty (provide bucket name and restart).");

        s3 = new AmazonS3Client(cred);

        if (!s3.doesBucketExist(bucketName)) {
            try {
                s3.createBucket(bucketName);

                BucketVersioningConfiguration verCfg = new BucketVersioningConfiguration();

                verCfg.setStatus(BucketVersioningConfiguration.ENABLED);

                s3.setBucketVersioningConfiguration(new SetBucketVersioningConfigurationRequest(bucketName, verCfg));

                println("Created S3 bucket: " + bucketName);

                while (!s3.doesBucketExist(bucketName))
                    try {
                        TimeUnit.MILLISECONDS.sleep(200L);
                    }
                    catch (Exception e) {
                        throw new RuntimeException("Thread has been interrupted.", e);
                    }
            }
            catch (AmazonClientException e) {
                if (!s3.doesBucketExist(bucketName)) {
                    s3 = null;

                    throw new RuntimeException("Failed to create bucket: " + bucketName, e);
                }
            }
        }

        // Client init success.
        clientInit = true;
    }

    /**
     * Replace all <i>/</i> to <i>#</i> .
     *
     * @param url url to master node.
     * @return s3 key.
     */
    private String urlToS3Key(String url) {
        return url.replaceAll("/", "#");
    }

    /**
     * Replace all <i>#</i> to <i>/</i> .
     *
     * @param s3Key s3 key.
     * @return url to master node.
     */
    private String s3KeyToUrl(String s3Key){
        return s3Key.replaceAll("#", "/");
    }
}

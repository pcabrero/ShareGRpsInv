package com.springml.salesforce.wave.impl;

import static com.springml.salesforce.wave.util.WaveAPIConstants.*;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import com.springml.salesforce.wave.model.BatchResultList;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.log4j.Logger;

import com.sforce.soap.partner.PartnerConnection;
import com.springml.salesforce.wave.api.BulkAPI;
import com.springml.salesforce.wave.model.BatchInfo;
import com.springml.salesforce.wave.model.BatchInfoList;
import com.springml.salesforce.wave.model.JobInfo;
import com.springml.salesforce.wave.util.LRUCache;
import com.springml.salesforce.wave.util.SFConfig;

public class BulkAPIImpl extends AbstractAPIImpl implements BulkAPI {
    private static final Logger LOG = Logger.getLogger(BulkAPIImpl.class);
    private Map<String, String> jobContentTypeMap = new LRUCache<String, String>(100);

    public BulkAPIImpl(SFConfig sfConfig) throws Exception {
        super(sfConfig);
    }

    public JobInfo createJob(String object) throws Exception {
        JobInfo jobInfo = new JobInfo(STR_CSV, object, STR_UPDATE);

        return createJob(jobInfo);
    }

    public JobInfo createJob(String object, String operation, String contentType) throws Exception {
        JobInfo jobInfo = new JobInfo(contentType, object, operation);

        return createJob(jobInfo);
    }

    public JobInfo createJob(JobInfo jobInfo) throws Exception {
        return createJob(jobInfo, new ArrayList<Header>());
    }

    public JobInfo createJob(JobInfo jobInfo, List<Header> customHeaders) throws Exception {
        PartnerConnection connection = getSfConfig().getPartnerConnection();
        URI requestURI = getSfConfig().getRequestURI(connection, getJobPath());

        String response = getHttpHelper().post(requestURI, getSfConfig().getSessionId(),
                getObjectMapper().writeValueAsString(jobInfo), true, customHeaders);
        LOG.debug("Response from Salesforce Server " + response);

        JobInfo respJobInfo = getObjectMapper().readValue(response.getBytes(), JobInfo.class);
        jobContentTypeMap.put(respJobInfo.getId(), getRespectiveCntType(jobInfo));

        return respJobInfo;
    }

    public BatchInfo addBatch(String jobId, String csvContent) throws Exception {
        PartnerConnection connection = getSfConfig().getPartnerConnection();
        URI requestURI = getSfConfig().getRequestURI(connection, getBatchPath(jobId));

        String contentType = getContentType(jobId);
        String response = getHttpHelper().post(requestURI, getSfConfig().getSessionId(),
                csvContent, contentType, true);
        LOG.debug("Response from Salesforce Server " + response);

        // Response is in xml though Accept is set to application/json
        if (CONTENT_TYPE_APPLICATION_JSON.equals(contentType)) {
            return getObjectMapper().readValue(response.getBytes(), BatchInfo.class);
        }

        return getXmlMapper().readValue(response.getBytes(), BatchInfo.class);
    }

    public JobInfo closeJob(String jobId) throws Exception {
        PartnerConnection connection = getSfConfig().getPartnerConnection();
        URI requestURI = getSfConfig().getRequestURI(connection, getJobPath(jobId));

        JobInfo jobInfo = new JobInfo(STR_CLOSED);
        String response = getHttpHelper().post(requestURI, getSfConfig().getSessionId(),
                getObjectMapper().writeValueAsString(jobInfo), true);
        LOG.debug("Response from Salesforce Server " + response);

        return getObjectMapper().readValue(response.getBytes(), JobInfo.class);
    }

    public boolean isCompleted(String jobId) throws Exception {
        BatchInfoList batchInfoList = getBatchInfoList(jobId);
        List<BatchInfo> batchInfos = batchInfoList.getBatchInfo();

        LOG.debug("BatchInfos : " + batchInfos);
        if (batchInfos != null) {
            for (BatchInfo batchInfo : batchInfos) {
                LOG.debug("Batch state : " + batchInfo.getState());
                // The following reference details all the different batch states:
                // https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_batches_interpret_status.htm
                if (STR_FAILED.equals(batchInfo.getState())) {
                    throw new Exception("Batch '" + batchInfo.getId() + "' failed with error '" + batchInfo.getStateMessage() + "'");
                } else if (STR_IN_PROGRESS.equals(batchInfo.getState()) || STR_QUEUED.equals(batchInfo.getState())) {
                    return false;
                }

                LOG.info("Number of records failed : " + batchInfo.getNumberRecordsFailed());
                if (batchInfo.getNumberRecordsFailed() > 0) {
                    String result = getResult(jobId, batchInfo.getId());
                    LOG.error("Failed record details \n " + result);
                    throw new Exception("Batch '" + batchInfo.getId() +
                            "' failed. Number of failed records is " + batchInfo.getNumberRecordsFailed());
                }
            }
        }

        return true;
    }

    private String getResult(String jobId, String batchId) throws Exception {
        PartnerConnection connection = getSfConfig().getPartnerConnection();
        URI requestURI = getSfConfig().getRequestURI(connection, getBatchResultPath(jobId, batchId));
        return getHttpHelper().get(requestURI, getSfConfig().getSessionId(), true);
    }

    public BatchInfoList getBatchInfoList(String jobId) throws Exception {
        PartnerConnection connection = getSfConfig().getPartnerConnection();
        URI requestURI = getSfConfig().getRequestURI(connection, getBatchPath(jobId));

        String response = getHttpHelper().get(requestURI, getSfConfig().getSessionId(), true);
        LOG.debug("Response from Salesforce Server " + response);

        if (CONTENT_TYPE_APPLICATION_JSON.equals(getContentType(jobId))) {
            return getObjectMapper().readValue(response.getBytes(), BatchInfoList.class);
        }

        return getXmlMapper().readValue(response.getBytes(), BatchInfoList.class);
    }

    public BatchInfo getBatchInfo(String jobId, String batchId) throws Exception {
        PartnerConnection connection = getSfConfig().getPartnerConnection();
        URI requestURI = getSfConfig().getRequestURI(connection, getBatchPath(jobId, batchId));

        String response = getHttpHelper().get(requestURI, getSfConfig().getSessionId(), true);
        LOG.debug("Response from Salesforce Server " + response);

        return getXmlMapper().readValue(response.getBytes(), BatchInfo.class);
    }

    public List<String> getBatchResultIds(String jobId, String batchId) throws Exception {
        String response = getResult(jobId, batchId);

        if (CONTENT_TYPE_APPLICATION_JSON.equals(getContentType(jobId))) {
            if (response != null && response.startsWith("[") && response.endsWith("]")) {
                return Arrays.asList(response.substring(1, response.length() - 1).split(","));
            } else {
                throw new Exception("Unable to parse response: " + response);
            }
        }

        return getXmlMapper().readValue(response.getBytes(), BatchResultList.class).getBatchResultIds();
    }

    public String getBatchResult(String jobId, String batchId, String resultId) throws Exception {
        PartnerConnection connection = getSfConfig().getPartnerConnection();
        URI requestURI = getSfConfig().getRequestURI(connection, getBatchResultPath(jobId, batchId, resultId));

        String response = getHttpHelper().get(requestURI, getSfConfig().getSessionId(), true);
        LOG.debug("Response from Salesforce Server " + response);

        return response;
    }

    public boolean isSuccess(String jobId) throws Exception {
        // TODO Auto-generated method stub
        return false;
    }

    private String getContentType(String jobId) {
        String contentType = jobContentTypeMap.get(jobId);
        if (StringUtils.isEmpty(contentType)){
            contentType = CONTENT_TYPE_TEXT_CSV;
        }

        return contentType;
    }

    private String getRespectiveCntType(JobInfo jobInfo) {
        String contentType = null;
        if (STR_JSON.equals(jobInfo.getContentType())) {
            contentType = CONTENT_TYPE_APPLICATION_JSON;
        } else if (STR_XML.equals(jobInfo.getContentType())) {
            contentType = CONTENT_TYPE_APPLICATION_XML;
        } else {
            contentType = CONTENT_TYPE_TEXT_CSV;
        }

        return contentType;
    }

    private String getBatchPath(String jobId, String batchId) {
        StringBuilder batchPath = new StringBuilder();
        batchPath.append(getBatchPath(jobId));
        batchPath.append('/');
        batchPath.append(batchId);
        return batchPath.toString();
    }

    private String getBatchResultPath(String jobId, String batchId) {
        StringBuilder batchResultPath = new StringBuilder();
        batchResultPath.append(getBatchPath(jobId, batchId));
        batchResultPath.append(PATH_RESULT);

        return batchResultPath.toString();
    }

    private String getBatchResultPath(String jobId, String batchId, String resultId) {
        StringBuilder batchResultPath = new StringBuilder();
        batchResultPath.append(getBatchResultPath(jobId, batchId));
        batchResultPath.append('/');
        batchResultPath.append(resultId);

        return batchResultPath.toString();
    }

    private String getBatchPath(String jobId) {
        StringBuilder batchPath = new StringBuilder();
        batchPath.append(getJobPath(jobId));
        batchPath.append(PATH_BATCH);

        return batchPath.toString();
    }

    private String getJobPath(String jobId) {
        StringBuilder jobPath = new StringBuilder();
        jobPath.append(getJobPath());
        jobPath.append('/');
        jobPath.append(jobId);
        return jobPath.toString();
    }

    private String getJobPath() {
        StringBuilder jobPath = new StringBuilder();
        jobPath.append(SERVICE_ASYNC_PATH);
        jobPath.append(getSfConfig().getApiVersion());
        jobPath.append(PATH_JOB);
        return jobPath.toString();
    }
}

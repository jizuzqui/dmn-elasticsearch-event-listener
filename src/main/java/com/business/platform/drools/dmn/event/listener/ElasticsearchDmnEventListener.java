package com.business.platform.drools.dmn.event.listener;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.kie.dmn.api.core.DMNMessage;
import org.kie.dmn.api.core.ast.BusinessKnowledgeModelNode;
import org.kie.dmn.api.core.ast.DecisionNode;
import org.kie.dmn.api.core.event.AfterEvaluateAllEvent;
import org.kie.dmn.api.core.event.AfterEvaluateBKMEvent;
import org.kie.dmn.api.core.event.AfterEvaluateContextEntryEvent;
import org.kie.dmn.api.core.event.AfterEvaluateDecisionEvent;
import org.kie.dmn.api.core.event.AfterEvaluateDecisionServiceEvent;
import org.kie.dmn.api.core.event.AfterEvaluateDecisionTableEvent;
import org.kie.dmn.api.core.event.AfterInvokeBKMEvent;
import org.kie.dmn.api.core.event.BeforeEvaluateAllEvent;
import org.kie.dmn.api.core.event.BeforeEvaluateBKMEvent;
import org.kie.dmn.api.core.event.BeforeEvaluateContextEntryEvent;
import org.kie.dmn.api.core.event.BeforeEvaluateDecisionEvent;
import org.kie.dmn.api.core.event.BeforeEvaluateDecisionServiceEvent;
import org.kie.dmn.api.core.event.BeforeEvaluateDecisionTableEvent;
import org.kie.dmn.api.core.event.BeforeInvokeBKMEvent;
import org.kie.dmn.api.core.event.DMNEvent;
import org.kie.dmn.api.core.event.DMNRuntimeEventListener;
import org.kie.dmn.api.feel.runtime.events.FEELEvent;
import org.kie.dmn.api.feel.runtime.events.FEELEventListener;
import org.kie.dmn.model.api.InformationRequirement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class ElasticsearchDmnEventListener implements DMNRuntimeEventListener, FEELEventListener {

	private static Logger logger = LoggerFactory.getLogger(ElasticsearchDmnEventListener.class);


	private String dateFormatStr = System.getProperty("org.jbpm.event.emitters.elasticsearch.date_format", System.getProperty("org.kie.server.json.date_format", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"));
	private String elasticSearchUrl = System.getProperty("org.jbpm.event.emitters.elasticsearch.url", "http://localhost:9200");
	private String elasticSearchUser = System.getProperty("org.jbpm.event.emitters.elasticsearch.user");
	private String elasticSearchPassword = System.getProperty("org.jbpm.event.emitters.elasticsearch.password");

	private CloseableHttpClient httpclient;
	private ObjectMapper mapper = new ObjectMapper();

	public ElasticsearchDmnEventListener() {
		mapper.setDateFormat(new SimpleDateFormat(dateFormatStr));
		mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		mapper.configure(MapperFeature.PROPAGATE_TRANSIENT_MARKER, true);
		httpclient = buildClient();
		
		
		File file = new File("R:\\aa\\aa");
	}

	@Override
	public void onEvent(FEELEvent feelEvent) {
		//		logger.debug("---------------------------------------");
		//		logger.debug("Feel event START");
		//		logger.debug("Feel event Message: " + feelEvent.getMessage());
		//		logger.debug("Feel event Line: " + feelEvent.getLine());
		//		logger.debug("Feel event Column: " + feelEvent.getColumn());
		//		logger.debug("Feel event END");
		//		logger.debug("---------------------------------------");
	}

	@Override
	public void beforeEvaluateDecision(BeforeEvaluateDecisionEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void afterEvaluateDecision(AfterEvaluateDecisionEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());

		sendEventToElasticsearch(event);
	}

	@Override
	public void beforeEvaluateBKM(BeforeEvaluateBKMEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void afterEvaluateBKM(AfterEvaluateBKMEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
		
		sendEventToElasticsearch(event);
	}

	@Override
	public void beforeEvaluateContextEntry(BeforeEvaluateContextEntryEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void afterEvaluateContextEntry(AfterEvaluateContextEntryEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void beforeEvaluateDecisionTable(BeforeEvaluateDecisionTableEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void afterEvaluateDecisionTable(AfterEvaluateDecisionTableEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void beforeEvaluateDecisionService(BeforeEvaluateDecisionServiceEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void afterEvaluateDecisionService(AfterEvaluateDecisionServiceEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void beforeInvokeBKM(BeforeInvokeBKMEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void afterInvokeBKM(AfterInvokeBKMEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void beforeEvaluateAll(BeforeEvaluateAllEvent event) {
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	@Override
	public void afterEvaluateAll(AfterEvaluateAllEvent event) {
//		DMNRuntimeEventListener.super.afterEvaluateAll(event);
		logger.debug("DMN Event " + Thread.currentThread().getStackTrace()[1].getMethodName());
	}

	protected CloseableHttpClient buildClient() {

		HttpClientBuilder builder = HttpClients.custom();

		if (elasticSearchUser != null && elasticSearchPassword != null) {
			CredentialsProvider provider = new BasicCredentialsProvider();
			UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(elasticSearchUser, elasticSearchPassword);
			provider.setCredentials(AuthScope.ANY, credentials);

			builder.setDefaultCredentialsProvider(provider);
		}

		return builder.build();
	}

	protected void closeListener () {
		try {
			httpclient.close();
		} catch (IOException e) {
			logger.error("Error when closing http client", e);
		}
	}

	protected StringBuilder buildMessage (DMNEvent dmnEvent) {
		
		String eventBusinessData = "";
		String eventMessages = "";
		String elasticsearchEventId = "";
		String index = "dmn-models";
		
		StringBuilder content = null;
		
		if(dmnEvent instanceof AfterEvaluateDecisionEvent) {
			AfterEvaluateDecisionEvent castedEvent = (AfterEvaluateDecisionEvent)dmnEvent;
						
			DecisionNode decisionNode = castedEvent.getDecision();
			Object dmnDecisionResult = castedEvent.getResult().getDecisionResultById(decisionNode.getId()).getResult();
			List<DMNMessage> dmnDecisionResultMessages = castedEvent.getResult().getDecisionResultById(decisionNode.getId()).getMessages();
			
			logger.debug("DMN Event Decision node name: " + castedEvent.getDecision().getName());
			logger.debug("DMN Event Decision node id: " + castedEvent.getDecision().getId());
			logger.debug("DMN Event Decision ResultType Name: " + castedEvent.getDecision().getResultType().getName());
			logger.debug("DMN Event Decision Status: " + castedEvent.getResult().getDecisionResultById(castedEvent.getDecision().getId()).getEvaluationStatus().name());
			logger.debug("DMN Event Decision Result: " + castedEvent.getResult().getDecisionResultById(castedEvent.getDecision().getId()).getResult());
		
			Map<String, Object> contextObjects = castedEvent.getResult().getContext().getAll();
			contextObjects.remove(castedEvent.getDecision().getId());
			
			elasticsearchEventId = elasticsearchEventId.concat(castedEvent.getDecision().getName());
			
			try {
				eventBusinessData =  
						"{\"decision-name\":\"" + castedEvent.getDecision().getName() + "\"" + 
						",\"decision-inputs\":" + mapper.writeValueAsString(contextObjects) +
						",\"decision-results\":" + 
							"{\"" + castedEvent.getDecision().getName() + "\":" + mapper.writeValueAsString(dmnDecisionResult) + "}" + 
						"}";
				
				String type = "dmn";
				
				elasticsearchEventId = elasticsearchEventId.concat(String.valueOf(Calendar.getInstance().getTimeInMillis()));
			} catch (JsonProcessingException e) {
				e.printStackTrace();
				logger.error("Error while serializing {} to JSON", dmnDecisionResult, e);
			}
		}
		else if(dmnEvent instanceof AfterEvaluateBKMEvent) {
			AfterEvaluateBKMEvent castedEvent = (AfterEvaluateBKMEvent)dmnEvent;
			
			BusinessKnowledgeModelNode businessKnowledgeModelNode = castedEvent.getBusinessKnowledgeModel();
			Object bkmResult = castedEvent.getResult().getDecisionResultById(businessKnowledgeModelNode.getId()).getResult();
			List<DMNMessage> dmnDecisionResultMessages = castedEvent.getResult().getDecisionResultById(businessKnowledgeModelNode.getId()).getMessages();
			
			logger.debug("DMN Event BKM node name: " + businessKnowledgeModelNode.getName());
			logger.debug("DMN Event BKM node id: " + businessKnowledgeModelNode.getId());
			logger.debug("DMN Event BKM ResultType Name: " + businessKnowledgeModelNode.getResultType().getName());
			logger.debug("DMN Event BKM Status: " + castedEvent.getResult().getDecisionResultById(businessKnowledgeModelNode.getId()).getEvaluationStatus().name());
			logger.debug("DMN Event BKM Result: " + bkmResult);
		}
		
		content = new StringBuilder();
		content.append("{ \"index\" : { \"_index\" : \"" + index + "\", \"_id\" : \"" + elasticsearchEventId + "\" } }\n");
		content.append(eventBusinessData);
		content.append("\n");
		
		return content;
	}

	protected void sendEventToElasticsearch (DMNEvent dmnEvent) {
				
		String eventData = buildMessage(dmnEvent).toString();

		try {
			logger.info("About sending event data to elasticsearch: " + eventData);
			
			HttpPut httpPut = new HttpPut(elasticSearchUrl + "/_bulk");
			httpPut.setEntity(new StringEntity(eventData, "UTF-8"));

			logger.debug("Executing request " + httpPut.getRequestLine());
			httpPut.setHeader("Content-Type", "application/x-ndjson");

			// Create a custom response handler
			ResponseHandler<String> responseHandler = response -> {
				int status = response.getStatusLine().getStatusCode();
				if (status >= 200 && status < 300) {
					HttpEntity entity = response.getEntity();
					return entity != null ? EntityUtils.toString(entity) : null;
				} else {
					throw new ClientProtocolException("Unexpected response status: " + status);
				}
			};
			String responseBody = httpclient.execute(httpPut, responseHandler);
			logger.info("Elastic search response '{}'", responseBody);
		} catch (Exception e) {
			logger.error("Unexpected exception while sending data to ElasticSearch", e);
		}	
	}

}

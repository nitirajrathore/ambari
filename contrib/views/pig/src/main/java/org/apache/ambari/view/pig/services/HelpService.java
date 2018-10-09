/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ambari.view.pig.services;

import org.apache.ambari.view.ViewContext;
import org.apache.ambari.view.ViewResourceHandler;
import org.apache.ambari.view.pig.persistence.DataStoreStorage;
import org.apache.ambari.view.pig.resources.files.FileService;
import org.apache.ambari.view.pig.resources.jobs.JobResourceManager;
import org.apache.ambari.view.pig.utils.ServiceCheck;
import org.apache.ambari.view.pig.utils.ServiceFormattedException;
import org.apache.ambari.view.utils.hdfs.HdfsApiException;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorPlan;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.pig.tools.pigstats.InputStats;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;
import org.apache.pig.PigRunner;
import org.apache.pig.PigRunner.ReturnCode;


/**
 * Help service
 */
public class HelpService extends BaseService {
  private final static Logger LOG =
    LoggerFactory.getLogger(HelpService.class);

  private ViewContext context;
  private ViewResourceHandler handler;

  /**
   * Constructor
   * @param context View Context instance
   * @param handler View Resource Handler instance
   */
  public HelpService(ViewContext context, ViewResourceHandler handler) {
    super();
    this.context = context;
    this.handler = handler;
  }

  /**
   * View configuration
   * @return configuration of HDFS
   */
  @GET
  @Path("/config")
  @Produces(MediaType.APPLICATION_JSON)
  public Response config(){
    JSONObject object = new JSONObject();
    String fs = context.getProperties().get("webhdfs.url");
    object.put("webhdfs.url", fs);
    return Response.ok(object).build();
  }

  /**
   * Version
   * @return version
   */
  @GET
  @Path("/version")
  @Produces(MediaType.TEXT_PLAIN)
  public Response version(){
    return Response.ok("0.0.1-SNAPSHOT").build();
  }

  // ================================================================================
  // Smoke tests
  // ================================================================================

  /**
   * HDFS Status
   * @return status
   */
  @GET
  @Path("/hdfsStatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response hdfsStatus(){
    FileService.hdfsSmokeTest(context);
    return getOKResponse();
  }


  /**
   * HomeDirectory Status
   * @return status
   */
  @GET
  @Path("/userhomeStatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response userhomeStatus (){
    FileService.userhomeSmokeTest(context);
    return getOKResponse();
  }


  /**
   * WebHCat Status
   * @return status
   */
  @GET
  @Path("/webhcatStatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response webhcatStatus(){
    JobResourceManager.webhcatSmokeTest(context);
    return getOKResponse();
  }

  /**
   * Storage Status
   * @return status
   */
  @GET
  @Path("/storageStatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response storageStatus(){
    DataStoreStorage.storageSmokeTest(context);
    return getOKResponse();
  }

  public static class MyPigProgressNotificationListener implements PigProgressNotificationListener {
    @Override
    public void initialPlanNotification(String s, OperatorPlan<?> operatorPlan) {
      LOG.info("nitiraj initialPlanNotification : s={}, operatorPlan={}", s, operatorPlan);
    }

    @Override
    public void launchStartedNotification(String s, int i) {
      LOG.info("nitiraj launchStartedNotification : s={}, i={}", s, i);
    }

    @Override
    public void jobsSubmittedNotification(String s, int i) {
      LOG.info("nitiraj jobsSubmittedNotification : s={}, i={}", s, i);
    }

    @Override
    public void jobStartedNotification(String s, String s1) {
      LOG.info("nitiraj jobStartedNotification : s={}, s1={}", s, s1);
    }

    @Override
    public void jobFinishedNotification(String s, JobStats jobStats) {
      LOG.info("nitiraj jobFinishedNotification : s={}, jobStats={}", s, jobStats);
    }

    @Override
    public void jobFailedNotification(String s, JobStats jobStats) {
      LOG.info("nitiraj jobFailedNotification : s={}, jobStats={}", s, jobStats);
    }

    @Override
    public void outputCompletedNotification(String s, OutputStats outputStats) {
      LOG.info("nitiraj outputCompletedNotification : s={}, outputStats={}", s, outputStats);
      try {
        Iterator<Tuple> iterator = outputStats.iterator();
        int i= 0 ;
        while(iterator.hasNext()){
          LOG.info( "nitiraj : result : {} : {}",i, iterator.next());
        }
      } catch (IOException e) {
        LOG.error("error occurred while iterating results : ", e);
      }
    }

    @Override
    public void progressUpdatedNotification(String s, int i) {
      LOG.info("nitiraj progressUpdatedNotification : s={}, i={}", s, i);
    }

    @Override
    public void launchCompletedNotification(String s, int i) {
      LOG.info("nitiraj launchCompletedNotification : s={}, i={}", s, i);
    }
  }
  /**
   * Get single item
   */
  @GET
  @Path("/exec")
  @Produces(MediaType.APPLICATION_JSON)
  public Response executePigQuery() {
    try {
      String query ="truck_events = LOAD '/tmp/truck_event_text_partition.csv' USING PigStorage(',')" +
          "AS (driverId:int, truckId:int, eventTime:chararray," +
          "eventType:chararray, longitude:double, latitude:double," +
          "eventKey:chararray, correlationId:long, driverName:chararray," +
          "routeId:long,routeName:chararray,eventDate:chararray);" +
          "DESCRIBE truck_events;" +
          "truck_events_subset = LIMIT truck_events 100;" +
          "DESCRIBE truck_events_subset;" +
          "DUMP truck_events_subset;";
      String[] args = new String[]{
          "-x", "tez",
          "-e", query
      };

      LOG.info("executing with query = {}", query);

      PigStats stats = PigRunner.run(args, new MyPigProgressNotificationListener());
      LOG.info("PigStats : {}", stats);
      return Response.ok().build();
    } catch (WebApplicationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new ServiceFormattedException(ex.getMessage(), ex);
    }
  }

  @GET
  @Path("/service-check-policy")
  public Response getServiceCheckList(){
    ServiceCheck serviceCheck = new ServiceCheck(context);
    try {
      ServiceCheck.Policy policy = serviceCheck.getServiceCheckPolicy();
      JSONObject policyJson = new JSONObject();
      policyJson.put("serviceCheckPolicy", policy);
      return Response.ok(policyJson).build();
    } catch (HdfsApiException e) {
      LOG.error("Error occurred while generating service check policy : ", e);
      throw new ServiceFormattedException(e);
    }
  }

  private Response getOKResponse() {
    JSONObject response = new JSONObject();
    response.put("message", "OK");
    response.put("trace", null);
    response.put("status", "200");
    return Response.ok().entity(response).type(MediaType.APPLICATION_JSON).build();
  }
}

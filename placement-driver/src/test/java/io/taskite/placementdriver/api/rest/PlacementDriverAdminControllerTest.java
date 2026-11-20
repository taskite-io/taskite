/*
 * Copyright (c) 2026 lzgabel and Taskite contributors
 *
 * Licensed under the Taskite Community License, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://github.com/taskite-io/taskite/blob/main/LICENSE
 *
 * The Software is source-available for non-commercial use and limited evaluation
 * use. Other commercial use requires a separate written license. The Software is
 * provided "AS IS", without warranties or conditions of any kind.
 */
package io.taskite.placementdriver.api.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.taskite.placementdriver.api.PlacementDriverEndpointManager;
import io.taskite.placementdriver.api.model.PlacementDriverHttpModel.JobSubscriptionMirrorView;
import io.taskite.placementdriver.api.model.PlacementDriverHttpModel.JobSubscriptionRedistributionAcceptedResponse;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.CreateJobSubscriptionMirrorRequest;
import io.taskite.placementdriver.api.model.PlacementDriverHttpRequestModel.RedistributeJobSubscriptionRequest;
import io.taskite.protocol.record.value.placementdriver.commanddistribution.JobSubscriptionDistributionStatus;
import io.taskite.protocol.record.value.placementdriver.mirror.JobSubscriptionMirrorStatus;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.request.async.DeferredResult;

final class PlacementDriverAdminControllerTest {

  @Test
  void exposesResourceListFiltersAndRedistributionBody() throws Exception {
    final var manager = mock(PlacementDriverEndpointManager.class);
    when(manager.queryJobSubscriptionDistributions("order", "9007199254741052", "demo"))
        .thenReturn(completed(ResponseEntity.ok().build()));
    when(manager.queryJobSubscriptionDistributions(null, null, null))
        .thenReturn(completed(ResponseEntity.ok().build()));
    when(manager.redistributeJobSubscription(
            "9007199254741052", new RedistributeJobSubscriptionRequest("9007199254741053")))
        .thenReturn(
            completed(
                ResponseEntity.accepted()
                    .body(
                        new JobSubscriptionRedistributionAcceptedResponse(
                            9007199254741052L,
                            9007199254741054L,
                            1793400000000L,
                            JobSubscriptionDistributionStatus.DISTRIBUTING))));
    final var mvc =
        MockMvcBuilders.standaloneSetup(new PlacementDriverAdminController(manager)).build();
    final var list =
        mvc.perform(
                get("/api/v1/broker/job-subscriptions")
                    .queryParam("jobType", "order")
                    .queryParam("distributionKey", "9007199254741052")
                    .queryParam("clusterId", "demo"))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(list)).andExpect(status().isOk());
    final var defaults =
        mvc.perform(get("/api/v1/broker/job-subscriptions"))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(defaults)).andExpect(status().isOk());
    final var write =
        mvc.perform(
                post("/api/v1/broker/job-subscriptions/9007199254741052/redistribute")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content("{\"expectedGeneration\":\"9007199254741053\"}"))
            .andExpect(request().asyncStarted())
            .andReturn();
    mvc.perform(asyncDispatch(write))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.generation").value(9007199254741054L));
    verify(manager).queryJobSubscriptionDistributions("order", "9007199254741052", "demo");
    verify(manager)
        .redistributeJobSubscription(
            "9007199254741052", new RedistributeJobSubscriptionRequest("9007199254741053"));
  }

  @Test
  void exposesBrokerClustersEndpoint() throws Exception {
    final PlacementDriverEndpointManager manager = mock(PlacementDriverEndpointManager.class);
    when(manager.queryBrokerClusters()).thenReturn(completed(ResponseEntity.ok().build()));
    final MockMvc mvc =
        MockMvcBuilders.standaloneSetup(new PlacementDriverAdminController(manager)).build();

    final MvcResult result =
        mvc.perform(get("/api/v1/broker/clusters")).andExpect(request().asyncStarted()).andReturn();

    mvc.perform(asyncDispatch(result)).andExpect(status().isOk());
    verify(manager).queryBrokerClusters();
  }

  @Test
  void exposesJobSubscriptionMirrorCreateEndpoint() throws Exception {
    final PlacementDriverEndpointManager manager = mock(PlacementDriverEndpointManager.class);
    when(manager.createJobSubscriptionMirror(any(CreateJobSubscriptionMirrorRequest.class)))
        .thenReturn(
            completed(
                ResponseEntity.status(201)
                    .body(
                        new JobSubscriptionMirrorView(
                            9001,
                            "region-a",
                            "region-b",
                            JobSubscriptionMirrorStatus.ACTIVE,
                            true))));
    final MockMvc mvc =
        MockMvcBuilders.standaloneSetup(new PlacementDriverAdminController(manager)).build();

    final MvcResult result =
        mvc.perform(
                post("/api/v1/broker/job-subscription-mirrors")
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(
                        """
                        {"sourceClusterId":"region-a","targetClusterId":"region-b"}
                        """))
            .andExpect(request().asyncStarted())
            .andReturn();

    mvc.perform(asyncDispatch(result))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.mirrorKey").value(9001))
        .andExpect(jsonPath("$.created").value(true));

    verify(manager)
        .createJobSubscriptionMirror(
            new CreateJobSubscriptionMirrorRequest("region-a", "region-b"));
  }

  @Test
  void exposesJobSubscriptionMirrorQueryAndDeleteEndpoints() throws Exception {
    final PlacementDriverEndpointManager manager = mock(PlacementDriverEndpointManager.class);
    when(manager.queryJobSubscriptionMirrors("region-a", "region-b"))
        .thenReturn(completed(ResponseEntity.ok().build()));
    when(manager.queryJobSubscriptionMirror("9001"))
        .thenReturn(completed(ResponseEntity.ok().build()));
    when(manager.deleteJobSubscriptionMirror("9001"))
        .thenReturn(completed(ResponseEntity.noContent().build()));
    when(manager.queryJobSubscriptionDistribution("1001"))
        .thenReturn(completed(ResponseEntity.ok().build()));
    final MockMvc mvc =
        MockMvcBuilders.standaloneSetup(new PlacementDriverAdminController(manager)).build();

    final MvcResult listResult =
        mvc.perform(
                get("/api/v1/broker/job-subscription-mirrors")
                    .queryParam("sourceClusterId", "region-a")
                    .queryParam("targetClusterId", "region-b"))
            .andExpect(request().asyncStarted())
            .andReturn();
    final MvcResult getResult =
        mvc.perform(get("/api/v1/broker/job-subscription-mirrors/9001"))
            .andExpect(request().asyncStarted())
            .andReturn();
    final MvcResult deleteResult =
        mvc.perform(delete("/api/v1/broker/job-subscription-mirrors/9001"))
            .andExpect(request().asyncStarted())
            .andReturn();
    final MvcResult distributionResult =
        mvc.perform(get("/api/v1/broker/job-subscriptions/1001"))
            .andExpect(request().asyncStarted())
            .andReturn();

    mvc.perform(asyncDispatch(listResult)).andExpect(status().isOk());
    mvc.perform(asyncDispatch(getResult)).andExpect(status().isOk());
    mvc.perform(asyncDispatch(deleteResult)).andExpect(status().isNoContent());
    mvc.perform(asyncDispatch(distributionResult)).andExpect(status().isOk());

    verify(manager).queryJobSubscriptionMirrors("region-a", "region-b");
    verify(manager).queryJobSubscriptionMirror("9001");
    verify(manager).deleteJobSubscriptionMirror("9001");
    verify(manager).queryJobSubscriptionDistribution("1001");
  }

  private static DeferredResult<ResponseEntity<Object>> completed(
      final ResponseEntity<?> response) {
    final DeferredResult<ResponseEntity<Object>> result = new DeferredResult<>();
    @SuppressWarnings("unchecked")
    final ResponseEntity<Object> typed = (ResponseEntity<Object>) response;
    result.setResult(typed);
    return result;
  }
}

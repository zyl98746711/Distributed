package com.study.distributed.manager;

import com.study.distributed.common.model.Result;
import com.study.distributed.common.serializer.JsonSerializer;
import com.study.distributed.manager.client.NodeHttpClient;
import com.study.distributed.manager.cluster.LeaderLocator;
import com.study.distributed.manager.cluster.ManagedNode;
import com.study.distributed.manager.cluster.NodeProcessManager;
import com.study.distributed.manager.config.ManagerProperties.NodeDefinition;
import com.study.distributed.manager.controller.ManagerController;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.ResourceAccessException;

import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Web 层不启动节点，不读写真实部署清单。 */
@WebMvcTest(ManagerController.class)
class ManagerWebTest {
    @Autowired private MockMvc mvc;
    @MockBean private NodeProcessManager processes;
    @MockBean private LeaderLocator leaders;
    @MockBean private NodeHttpClient http;
    private final NodeDefinition definition = new NodeDefinition("node-1", "127.0.0.1", 9001, 8001);

    @Test void staticConsoleIsAvailable() throws Exception {
        mvc.perform(get("/")).andExpect(status().isOk()).andExpect(forwardedUrl("index.html"));
        mvc.perform(get("/index.html")).andExpect(status().isOk())
                .andExpect(result -> assertTrue(result.getResponse().getContentAsString(StandardCharsets.UTF_8).contains("集群总览")));
        mvc.perform(get("/styles.css")).andExpect(status().isOk());
        mvc.perform(get("/app.js")).andExpect(status().isOk());
        verifyNoInteractions(processes, http);
    }

    @ParameterizedTest
    @CsvSource({"log,LOG", "kv,KV", "members,MEMBERS"})
    void observationUsesRegisteredNode(String path, NodeHttpClient.NodeView view) throws Exception {
        when(processes.require("node-1")).thenReturn(new ManagedNode(definition, ManagedNode.MembershipState.MEMBER));
        when(http.observe(definition, view)).thenReturn(Result.ok(JsonSerializer.getMapper().readTree("{\"observed\":true}")));
        mvc.perform(get("/manager/nodes/node-1/" + path)).andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200)).andExpect(jsonPath("$.data.observed").value(true));
        verify(http).observe(definition, view);
        verifyNoInteractions(leaders);
    }

    @ParameterizedTest
    @CsvSource({"log,LOG", "kv,KV", "members,MEMBERS"})
    void unknownNodeDoesNotMakeNetworkRequest(String path, NodeHttpClient.NodeView view) throws Exception {
        when(processes.require("node-99")).thenThrow(new NoSuchElementException("未知节点: node-99"));
        mvc.perform(get("/manager/nodes/node-99/" + path)).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value(404));
        verifyNoInteractions(http);
    }

    @Test void offlineNodeReturnsBadGateway() throws Exception {
        when(processes.require("node-1")).thenReturn(new ManagedNode(definition, ManagedNode.MembershipState.MEMBER));
        when(http.observe(definition, NodeHttpClient.NodeView.LOG)).thenThrow(new ResourceAccessException("离线"));
        mvc.perform(get("/manager/nodes/node-1/log")).andExpect(status().isBadGateway())
                .andExpect(jsonPath("$.code").value(502)).andExpect(jsonPath("$.message").value(containsString("node-1")));
    }

    @Test void upstreamBusinessErrorIsPreserved() throws Exception {
        when(processes.require("node-1")).thenReturn(new ManagedNode(definition, ManagedNode.MembershipState.MEMBER));
        when(http.observe(definition, NodeHttpClient.NodeView.MEMBERS)).thenReturn(Result.fail(503, "节点未就绪"));
        mvc.perform(get("/manager/nodes/node-1/members")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value(503)).andExpect(jsonPath("$.message").value("节点未就绪"));
    }
}

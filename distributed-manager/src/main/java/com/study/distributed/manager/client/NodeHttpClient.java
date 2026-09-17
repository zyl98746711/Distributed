package com.study.distributed.manager.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.study.distributed.common.model.Result;
import com.study.distributed.common.serializer.JsonSerializer;
import com.study.distributed.manager.config.ManagerProperties.NodeDefinition;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;
import java.net.URI;
import java.util.Map;

/** 所有网络调用有超时；转发写入结果未知时不自动重试。 */
@Component
public class NodeHttpClient {
    private final RestClient client = client(8000);
    private final RestClient probe = client(800);
    private static RestClient client(int readTimeout) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(500);
        factory.setReadTimeout(readTimeout);
        return RestClient.builder().requestFactory(factory).build();
    }
    private URI uri(NodeDefinition node, String path) {
        return UriComponentsBuilder.newInstance().scheme("http").host(node.host()).port(node.httpPort())
                .path(path).build().toUri();
    }
    public JsonNode status(NodeDefinition node) {
        Result<JsonNode> result = execute(probe, HttpMethod.GET, uri(node, "/node/status"), null);
        if (result.code() != 200 || result.data() == null
                || !node.id().equals(result.data().path("nodeId").asText())) {
            throw new IllegalStateException("端点不是预期节点: " + node.id());
        }
        return result.data();
    }
    public JsonNode members(NodeDefinition node) {
        Result<JsonNode> result = execute(client, HttpMethod.GET, uri(node, "/raft/members"), null);
        if (result.code() != 200 || result.data() == null) throw new IllegalStateException("成员查询失败");
        return result.data();
    }
    public Result<JsonNode> change(NodeDefinition leader, String action, NodeDefinition node) {
        return execute(client, HttpMethod.POST, uri(leader, "/raft/members"),
                Map.of("action", action, "id", node.id(), "host", node.host(), "rpcPort", node.rpcPort()));
    }
    public Result<JsonNode> kv(NodeDefinition leader, HttpMethod method, String key, String value) {
        var builder = UriComponentsBuilder.newInstance().scheme("http").host(leader.host()).port(leader.httpPort())
                .path("/kv/{key}");
        // 使用 URI 变量严格编码，避免 value 中的 &/+/# 改变查询含义。
        URI target = value == null ? builder.encode().buildAndExpand(key).toUri()
                : builder.queryParam("value", "{value}").encode().buildAndExpand(key, value).toUri();
        return execute(client, method, target, null);
    }
    private Result<JsonNode> execute(RestClient rest, HttpMethod method, URI uri, Object body) {
        var request = rest.method(method).uri(uri).header("X-Raft-Forwarded", "manager");
        if (body != null) request.contentType(org.springframework.http.MediaType.APPLICATION_JSON).body(body);
        return request.exchange((req, response) -> {
            var type = JsonSerializer.getMapper().getTypeFactory().constructParametricType(Result.class, JsonNode.class);
            Result<JsonNode> result = JsonSerializer.getMapper().readValue(response.getBody(), type);
            return result != null ? result : Result.fail(502, "节点响应为空");
        });
    }
}

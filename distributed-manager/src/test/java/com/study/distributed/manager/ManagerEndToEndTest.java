package com.study.distributed.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.study.distributed.common.serializer.JsonSerializer;
import com.study.distributed.manager.cluster.NodeProcessManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** 显式启用才启动真实 JVM；使用独立端口与部署状态，finally 清理本测试进程。 */
@EnabledIfSystemProperty(named = "cluster.e2e", matches = "true")
class ManagerEndToEndTest {
    private ConfigurableApplicationContext context;
    private int port;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final Set<Integer> ports = new HashSet<>();

    private int freePort() throws Exception {
        while (true) {
            try (ServerSocket socket = new ServerSocket(0)) {
                if (ports.add(socket.getLocalPort())) return socket.getLocalPort();
            }
        }
    }
    /** 与进程管理器使用相同的独占绑定检查，等待系统释放停止节点的端口。 */
    private void awaitPortsReleased(int... nodePorts) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            boolean available = true;
            for (int nodePort : nodePorts) {
                try (ServerSocket socket = new ServerSocket()) {
                    socket.setReuseAddress(false);
                    socket.bind(new InetSocketAddress("127.0.0.1", nodePort));
                } catch (BindException e) { available = false; }
            }
            if (available) return;
            Thread.sleep(200);
        }
        fail("停止节点的端口未在限定时间内释放");
    }
    private void start(String[] args) {
        context = new SpringApplicationBuilder(ManagerApplication.class).run(args);
        port = ((ServletWebServerApplicationContext) context).getWebServer().getPort();
    }
    private JsonNode call(String method, String path, String body, int expected) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(45)).header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        var response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(expected, response.statusCode(), response.body());
        JsonNode json = JsonSerializer.getMapper().readTree(response.body());
        assertEquals(expected, json.path("code").asInt(), response.body());
        return json.path("data");
    }
    private JsonNode cluster() throws Exception { return call("GET", "/manager/cluster", null, 200); }
    private JsonNode direct(int nodePort, String path) throws Exception {
        var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + nodePort + path))
                .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
        return JsonSerializer.getMapper().readTree(response.body()).path("data");
    }
    private void await(BooleanSupplier condition) throws Exception {
        long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < until) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(200);
        }
        fail("等待集群状态超时");
    }
    private String leader() {
        try { return cluster().path("leaderId").asText(""); }
        catch (Exception e) { return ""; }
    }

    @Test void processesGatewayMembershipFailoverAndManagerRestart() throws Exception {
        Path jar = Path.of("..", "distributed-node", "target", "distributed-node-1.0.0-SNAPSHOT.jar").toAbsolutePath().normalize();
        assertTrue(Files.isRegularFile(jar), "请从根工程运行 mvn package -Dcluster.e2e=true");
        Path work = Path.of("target", "e2e", UUID.randomUUID().toString()).toAbsolutePath();
        List<String> options = new ArrayList<>(List.of("--server.port=0", "--manager.node-jar=" + jar,
                "--manager.state-file=" + work.resolve("nodes.json"), "--manager.log-directory=" + work,
                "--manager.auto-start=true"));
        int[] httpPorts = new int[3];
        for (int i = 0; i < 3; i++) {
            httpPorts[i] = freePort();
            String prefix = "--manager.cluster.nodes[" + i + "].";
            options.add(prefix + "id=node-" + (i + 1));
            options.add(prefix + "host=127.0.0.1");
            options.add(prefix + "rpc-port=" + freePort());
            options.add(prefix + "http-port=" + httpPorts[i]);
        }
        String[] args = options.toArray(String[]::new);
        NodeProcessManager registry = null;
        try {
            start(args);
            registry = context.getBean(NodeProcessManager.class);
            for (String resource : List.of("/", "/styles.css", "/app.js")) {
                var page = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + resource))
                        .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertEquals(200, page.statusCode());
                assertFalse(page.body().isBlank());
                if ("/".equals(resource)) assertTrue(page.body().contains("集群总览"));
            }
            call("GET", "/manager/nodes/node-999/log", null, 404);
            await(() -> !leader().isEmpty());
            assertEquals(3, cluster().path("nodes").size());
            String value = "中文 + & # / =";
            call("PUT", "/manager/kv/name?value=" + URLEncoder.encode(value, java.nio.charset.StandardCharsets.UTF_8), null, 200);
            assertEquals(value, call("GET", "/manager/kv/name", null, 200).path("value").asText());
            await(() -> {
                try {
                    for (int i = 0; i < httpPorts.length; i++) {
                        if (!value.equals(direct(httpPorts[i], "/kv/name").path("value").asText())) return false;
                        if (!value.equals(call("GET", "/manager/nodes/node-" + (i + 1) + "/kv", null, 200).path("name").asText())) return false;
                    }
                    return true;
                } catch (Exception e) { return false; }
            });
            for (int i = 1; i <= 3; i++) {
                String base = "/manager/nodes/node-" + i;
                JsonNode observedLog = call("GET", base + "/log", null, 200);
                assertEquals("node-" + i, observedLog.path("nodeId").asText());
                assertFalse(observedLog.path("entries").isEmpty());
                assertEquals(3, call("GET", base + "/members", null, 200).path("committedMembers").size());
            }
            long pid = direct(httpPorts[0], "/node/status").path("pid").asLong();
            call("POST", "/manager/nodes/node-1/start", null, 200);
            assertEquals(pid, direct(httpPorts[0], "/node/status").path("pid").asLong());
            call("POST", "/manager/nodes", "{\"host\":\"192.0.2.1\"}", 400);
            assertEquals(3, cluster().path("nodes").size());

            JsonNode added = call("POST", "/manager/nodes", null, 200);
            int addedPort = added.path("httpPort").asInt();
            String addedId = added.path("id").asText();
            assertEquals(4, direct(addedPort, "/raft/members").path("members").size());
            assertEquals(value, direct(addedPort, "/kv/name").path("value").asText());
            assertFalse(direct(addedPort, "/raft/members").path("changing").asBoolean());
            call("PUT", "/manager/kv/expanded?value=yes", null, 200);

            // manager 重启时动态节点定义仍在，接管现有 PID，不重复拉起进程。
            long addedPid = direct(addedPort, "/node/status").path("pid").asLong();
            context.close();
            start(args);
            registry = context.getBean(NodeProcessManager.class);
            assertEquals(4, cluster().path("nodes").size());
            assertEquals(addedPid, direct(addedPort, "/node/status").path("pid").asLong());
            assertEquals(value, call("GET", "/manager/kv/name", null, 200).path("value").asText());

            call("DELETE", "/manager/nodes/" + addedId, null, 200);
            assertFalse(ProcessHandle.of(addedPid).map(ProcessHandle::isAlive).orElse(false));
            await(() -> {
                try { return direct(httpPorts[0], "/raft/members").path("members").size() == 3; }
                catch (Exception e) { return false; }
            });
            String former = leader();
            int restartedPort = httpPorts[Integer.parseInt(former.substring(5)) - 1];
            int restartedRpcPort = direct(restartedPort, "/node/status").path("rpcPort").asInt();
            call("POST", "/manager/nodes/" + former + "/stop", null, 200);
            call("GET", "/manager/nodes/" + former + "/log", null, 502);
            call("GET", "/manager/nodes/" + former + "/kv", null, 502);
            await(() -> { String current = leader(); return !current.isEmpty() && !current.equals(former); });
            call("PUT", "/manager/kv/failover?value=ok", null, 200);
            awaitPortsReleased(restartedPort, restartedRpcPort);
            call("POST", "/manager/nodes/" + former + "/start", null, 200);
            await(() -> {
                try { return "ok".equals(direct(restartedPort, "/kv/failover").path("value").asText()); }
                catch (Exception e) { return false; }
            });
            call("DELETE", "/manager/kv/name", null, 200);
            call("GET", "/manager/kv/name", null, 404);
            System.out.println("端到端通过：3 JVM / 网关编码与复制 / 幂等启动 / 扩缩容 / manager 接管 / Leader 故障与追赶");
        } finally {
            if (registry != null) {
                for (var node : registry.snapshot()) {
                    try { registry.stop(node); }
                    catch (Exception e) { System.err.println("测试节点清理失败: " + e.getMessage()); }
                }
            }
            if (context != null) context.close();
        }
    }
}

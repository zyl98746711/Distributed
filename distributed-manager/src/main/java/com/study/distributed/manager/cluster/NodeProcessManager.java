package com.study.distributed.manager.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.study.distributed.common.serializer.JsonSerializer;
import com.study.distributed.manager.client.NodeHttpClient;
import com.study.distributed.manager.config.ManagerProperties;
import com.study.distributed.manager.config.ManagerProperties.NodeDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import static com.study.distributed.manager.cluster.ManagedNode.MembershipState.*;
import static com.study.distributed.manager.cluster.ManagedNode.State.*;

/** 只操作本实例注册且核实身份的本机进程，不按 jar 名模糊扫描/杀进程。 */
@Component
public class NodeProcessManager {
    private static final Logger log = LoggerFactory.getLogger(NodeProcessManager.class);
    private final ManagerProperties properties;
    private final NodeHttpClient http;
    private final Map<String, ManagedNode> nodes = new ConcurrentSkipListMap<>();
    private final Path jar;
    private final Path stateFile;
    public record SavedNode(NodeDefinition definition, ManagedNode.MembershipState membership, String processToken) {}

    public NodeProcessManager(ManagerProperties properties, NodeHttpClient http) throws IOException {
        this.properties = properties;
        this.http = http;
        jar = Path.of(properties.nodeJar()).toAbsolutePath().normalize();
        stateFile = Path.of(properties.stateFile()).toAbsolutePath().normalize();
        List<SavedNode> saved = Files.exists(stateFile)
                ? JsonSerializer.getMapper().readValue(stateFile.toFile(), new TypeReference<List<SavedNode>>() {})
                : properties.cluster().nodes().stream().map(n -> new SavedNode(n, MEMBER, UUID.randomUUID().toString())).toList();
        if (saved.isEmpty()) throw new IllegalArgumentException("节点清单不能为空");
        for (SavedNode item : saved) {
            validateAvailable(item.definition());
            nodes.put(item.definition().id(), new ManagedNode(item.definition(), Objects.requireNonNull(item.membership()),
                    item.processToken()));
        }
        save();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void autoStart() {
        if (!properties.autoStart()) return;
        // 先发起所有进程，再逐个探活，避免串行等待选举形成启动依赖。
        for (ManagedNode node : snapshot()) {
            if (node.membership() == REMOVED) continue;
            try { start(node, false); }
            catch (Exception e) { log.error("启动 {} 失败: {}", node.definition().id(), e.getMessage()); }
        }
        for (ManagedNode node : snapshot()) {
            if (node.state() != STARTING) continue;
            try { awaitReady(node); }
            catch (Exception e) { log.error("节点未就绪: {}", e.getMessage()); }
        }
    }

    public List<ManagedNode> snapshot() { return List.copyOf(nodes.values()); }
    public ManagedNode require(String id) {
        ManagedNode node = nodes.get(id);
        if (node == null) throw new NoSuchElementException("未知节点: " + id);
        return node;
    }
    public synchronized ManagedNode reserve(String host, Integer rpcPort, Integer httpPort) throws IOException {
        int number = nodes.keySet().stream().mapToInt(id -> Integer.parseInt(id.substring(5))).max().orElse(0) + 1;
        int rpc = rpcPort == null ? availablePort(9000 + number, Set.of()) : rpcPort;
        int web = httpPort == null ? availablePort(8000 + number, Set.of(rpc)) : httpPort;
        NodeDefinition definition = new NodeDefinition("node-" + number, host == null ? "127.0.0.1" : host, rpc, web);
        validateAvailable(definition);
        checkPort(rpc);
        checkPort(web);
        ManagedNode node = new ManagedNode(definition, PENDING);
        nodes.put(definition.id(), node);
        save();
        return node;
    }
    private void validateAvailable(NodeDefinition definition) {
        for (ManagedNode node : snapshot()) {
            var other = node.definition();
            if (other.id().equals(definition.id()) || other.httpPort() == definition.httpPort()
                    || other.rpcPort() == definition.rpcPort() || other.rpcPort() == definition.httpPort()
                    || other.httpPort() == definition.rpcPort()) {
                throw new IllegalArgumentException("节点 ID 或端口重复: " + definition.id());
            }
        }
    }
    private int availablePort(int start, Set<Integer> reserved) {
        for (int port = Math.max(1024, start); port <= 65535; port++) {
            final int candidate = port;
            if (reserved.contains(port) || snapshot().stream().anyMatch(n -> n.definition().httpPort() == candidate
                    || n.definition().rpcPort() == candidate)) continue;
            try { checkPort(port); return port; } catch (IOException ignored) { }
        }
        throw new IllegalStateException("没有可用端口");
    }
    private void checkPort(int port) throws IOException {
        try (ServerSocket socket = new ServerSocket()) {
            socket.setReuseAddress(false);
            socket.bind(new InetSocketAddress("127.0.0.1", port));
        }
    }
    private String bootstrap() {
        String peers = snapshot().stream().filter(n -> n.membership() == MEMBER)
                .map(n -> n.definition().peerSpec()).collect(Collectors.joining(","));
        if (peers.isEmpty()) throw new IllegalStateException("没有可用的 bootstrap 成员配置");
        return peers;
    }

    public void start(ManagedNode node, boolean wait) throws Exception {
        synchronized (node) {
            if (node.membership() == REMOVED) throw new IllegalStateException("节点已被移除，不能直接重启");
            JsonNode live = null;
            try { live = http.status(node.definition()); } catch (Exception ignored) { }
            if (live != null) {
                ProcessHandle handle = ProcessHandle.of(live.path("pid").asLong()).orElseThrow();
                verifyIdentity(node, handle, live);
                node.process(handle);
                node.state(RUNNING);
                return;
            }
            if (node.process() == null || !node.process().isAlive()) {
                if (!Files.isRegularFile(jar)) throw new IllegalStateException("节点 jar 不存在，请先构建: " + jar);
                checkPort(node.definition().httpPort());
                checkPort(node.definition().rpcPort());
                Path directory = Path.of(properties.logDirectory()).toAbsolutePath();
                Files.createDirectories(directory);
                String java = Path.of(System.getProperty("java.home"), "bin",
                        System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java").toString();
                var d = node.definition();
                Process process = new ProcessBuilder(java, "-jar", jar.toString(),
                        "--server.address=127.0.0.1", "--server.port=" + d.httpPort(), "--raft.node-id=" + d.id(),
                        "--raft.host=" + d.host(), "--raft.rpc-port=" + d.rpcPort(), "--raft.peers=" + bootstrap(),
                        "--raft.process-token=" + node.processToken())
                        .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(
                                directory.resolve(d.id() + ".log").toFile())).start();
                node.process(process.toHandle());
                node.state(STARTING);
                log.info("启动 {}，PID={}", d.id(), process.pid());
            }
        }
        if (wait) awaitReady(node);
    }
    public void awaitReady(ManagedNode node) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(25);
        while (System.nanoTime() < deadline) {
            if (node.process() == null || !node.process().isAlive()) {
                node.state(STOPPED);
                throw new IllegalStateException("节点进程已退出，请检查 logs/" + node.definition().id() + ".log");
            }
            try {
                JsonNode status = http.status(node.definition());
                if (status.path("pid").asLong() != node.process().pid()) throw new IllegalStateException("PID 不匹配");
                node.state(RUNNING);
                return;
            } catch (Exception ignored) { }
            Thread.sleep(200);
        }
        throw new IllegalStateException("节点启动超时: " + node.definition().id());
    }
    private void verifyIdentity(ManagedNode node, ProcessHandle handle, JsonNode status) {
        // Windows 的 ProcessHandle.info().arguments() 可能为空，不依赖命令行解析。
        // 启动标识用于关联部署，不是鉴权凭据；创建时间进一步防止 PID 复用。
        String started = handle.info().startInstant().map(Object::toString).orElse("");
        if (!handle.isAlive() || started.isEmpty()
                || !started.equals(status.path("processStartedAt").asText())
                || !node.processToken().equals(status.path("processToken").asText())
                || status.path("pid").asLong() != handle.pid()
                || status.path("rpcPort").asInt() != node.definition().rpcPort()) {
            throw new IllegalStateException("进程身份不匹配，拒绝接管/停止 PID=" + handle.pid());
        }
    }
    public void stop(ManagedNode node) throws Exception {
        synchronized (node) {
            ProcessHandle process = node.process();
            if (process == null) {
                try {
                    JsonNode status = http.status(node.definition());
                    process = ProcessHandle.of(status.path("pid").asLong()).orElse(null);
                    if (process != null) verifyIdentity(node, process, status);
                } catch (org.springframework.web.client.RestClientException ignored) { }
            }
            if (process != null && process.isAlive()) {
                // ProcessHandle 内部同时保存 PID 与创建时间；已核验/亲自启动的句柄可用于停止。
                if (!process.equals(ProcessHandle.of(process.pid()).orElse(null))) {
                    throw new IllegalStateException("PID 已被复用，拒绝停止进程");
                }
                process.destroy();
                try { process.onExit().get(3, TimeUnit.SECONDS); }
                catch (java.util.concurrent.TimeoutException e) {
                    process.destroyForcibly();
                    process.onExit().get(3, TimeUnit.SECONDS);
                }
            }
            node.process(null);
            node.state(STOPPED);
        }
    }
    public synchronized void save() throws IOException {
        Files.createDirectories(stateFile.getParent());
        Path temp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
        JsonSerializer.getMapper().writeValue(temp.toFile(), snapshot().stream()
                .map(n -> new SavedNode(n.definition(), n.membership(), n.processToken())).toList());
        try { Files.move(temp, stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
        catch (AtomicMoveNotSupportedException e) { Files.move(temp, stateFile, StandardCopyOption.REPLACE_EXISTING); }
    }
}

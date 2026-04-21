package com.fbp.engine.node.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fbp.engine.core.Connection;
import com.fbp.engine.message.Message;
import com.fbp.engine.node.ConnectionState;
import com.fbp.engine.protocol.ModbusTcpSimulator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("ModbusReaderNode 테스트")
class ModbusReaderNodeTest {

    @Nested
    @DisplayName("단위 테스트")
    class UnitTest {
        private ModbusReaderNode node;

        @BeforeEach
        void setUp() {
            Map<String, Object> config = new HashMap<>();
            config.put("host", "localhost");
            config.put("port", 5020);
            config.put("slaveId", 1);
            config.put("startAddress", 0);
            config.put("count", 3);

            node = new ModbusReaderNode("reader", config);
        }

        @Test
        void testPorts() {
            assertNotNull(node.getInputPort("trigger"));
            assertNotNull(node.getOutputPort("out"));
            assertNotNull(node.getOutputPort("error"));
        }

        @Test
        void testInitialState() {
            assertEquals(ConnectionState.DISCONNECTED, node.getConnectionState());
            assertFalse(node.isConnected());
        }

        @Test
        void testConfig() {
            assertEquals("localhost", node.getConfig("host"));
            assertEquals(1, node.getConfig("slaveId"));
        }
    }

    @Nested
    @DisplayName("통합 테스트")
    class IntegrationTest {
        private ModbusTcpSimulator simulator;
        private static int portCounter = 27020;
        private int currentPort;

        @BeforeEach
        void setUp() throws Exception {
            currentPort = portCounter++;
            simulator = new ModbusTcpSimulator(currentPort, 10);
            simulator.start();
            Thread.sleep(200);

            simulator.setRegister(0, 250);
            simulator.setRegister(1, 600);
            simulator.setRegister(2, 1);
        }

        @AfterEach
        void tearDown() {
            if (simulator != null) {
                simulator.stop();
            }
        }

        private Map<String, Object> createBaseConfig() {
            Map<String, Object> config = new HashMap<>();
            config.put("host", "localhost");
            config.put("port", currentPort);
            config.put("slaveId", 1);
            config.put("startAddress", 0);
            config.put("count", 2);
            config.put("reconnectIntervalMs", 100);
            config.put("maxRetries", 3);
            return config;
        }

        @Test
        void testConnectionSuccess() {
            Map<String, Object> config = createBaseConfig();
            ModbusReaderNode node = new ModbusReaderNode("reader", config);

            node.initialize();

            assertTrue(node.isConnected());
            assertEquals(ConnectionState.CONNECTED, node.getConnectionState());

            node.shutdown();
        }

        @Test
        void testReadRegistersWithoutMapping() throws Exception {
            Map<String, Object> config = createBaseConfig();
            ModbusReaderNode node = new ModbusReaderNode("reader", config);
            CollectorNode collector = new CollectorNode("collector");

            Connection connection = new Connection("outToIn");
            connection.setTarget(collector.getInputPort("in"));
            node.getOutputPort("out").connect(connection);

            node.initialize();

            node.process(new Message(new HashMap<>()));
            Thread.sleep(100);

            Message processedMessage = connection.poll();
            if (processedMessage != null) {
                collector.process(processedMessage);
            }

            List<Message> messages = collector.getCollected();
            assertEquals(1, messages.size());

            Message msg = messages.get(0);

            @SuppressWarnings("unchecked")
            Map<String, Integer> registers = (Map<String, Integer>) msg.getPayload().get("registers");

            assertNotNull(registers);
            assertEquals(250, registers.get("0"));
            assertEquals(600, registers.get("1"));

            node.shutdown();
        }

        @Test
        void testReadRegistersWithMapping() throws Exception {
            Map<String, Object> config = createBaseConfig();
            Map<String, Object> mapping = new HashMap<>();
            mapping.put("0", Map.of("name", "temperature", "scale", 0.1));
            mapping.put("1", Map.of("name", "humidity", "scale", 0.1));
            config.put("registerMapping", mapping);

            ModbusReaderNode node = new ModbusReaderNode("reader", config);
            CollectorNode collector = new CollectorNode("collector");

            Connection connection = new Connection("outToIn");
            connection.setTarget(collector.getInputPort("in"));
            node.getOutputPort("out").connect(connection);

            node.initialize();

            node.process(new Message(new HashMap<>()));
            Thread.sleep(100);

            Message processedMessage = connection.poll();
            if (processedMessage != null) {
                collector.process(processedMessage);
            }

            List<Message> messages = collector.getCollected();
            assertEquals(1, messages.size());

            Message msg = messages.get(0);
            assertEquals(25.0, (Double) msg.getPayload().get("temperature"), 0.01);
            assertEquals(60.0, (Double) msg.getPayload().get("humidity"), 0.01);

            node.shutdown();
        }

        @Test
        void testErrorPortOnReadFailure() throws Exception {
            Map<String, Object> config = createBaseConfig();
            config.put("startAddress", 100);

            ModbusReaderNode node = new ModbusReaderNode("reader", config);
            CollectorNode errorCollector = new CollectorNode("errorCollector");

            Connection errorConnection = new Connection("errorToIn");
            errorConnection.setTarget(errorCollector.getInputPort("in"));
            node.getOutputPort("error").connect(errorConnection);

            node.initialize();

            node.process(new Message(Map.of("triggerId", 999)));
            Thread.sleep(100);

            Message processedMessage = errorConnection.poll();
            if (processedMessage != null) {
                errorCollector.process(processedMessage);
            }

            List<Message> errorMessages = errorCollector.getCollected();
            assertEquals(1, errorMessages.size());

            Message msg = errorMessages.get(0);
            assertNotNull(msg.getPayload().get("error"));

            @SuppressWarnings("unchecked")
            Map<String, Object> triggerMsg = (Map<String, Object>) msg.getPayload().get("triggerMessage");
            assertEquals(999, triggerMsg.get("triggerId"));

            node.shutdown();
        }

        @Test
        void testShutdown() {
            Map<String, Object> config = createBaseConfig();
            ModbusReaderNode node = new ModbusReaderNode("reader", config);

            node.initialize();
            assertTrue(node.isConnected());

            node.shutdown();
            assertFalse(node.isConnected());
            assertEquals(ConnectionState.DISCONNECTED, node.getConnectionState());
        }
    }
}
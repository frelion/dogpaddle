package dev.dogpaddle.debezium;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class RuntimeDependencyAlignmentTest {
    @Test
    void jackson_core_and_databind_share_a_version() {
        ObjectMapper mapper = new ObjectMapper();
        Version core = mapper.getFactory().version();
        Version databind = mapper.version();

        assertEquals(core.getMajorVersion(), databind.getMajorVersion());
        assertEquals(core.getMinorVersion(), databind.getMinorVersion());
        assertEquals(core.getPatchLevel(), databind.getPatchLevel());
    }

    @Test
    void slf4j_uses_the_packaged_simple_logger() {
        assertEquals(
                "org.slf4j.impl.SimpleLoggerFactory",
                LoggerFactory.getILoggerFactory().getClass().getName());
    }

    @Test
    void mysql_connector_class_is_available() throws Exception {
        assertEquals(
                "io.debezium.connector.mysql.MySqlConnector",
                Class.forName("io.debezium.connector.mysql.MySqlConnector").getName());
    }
}

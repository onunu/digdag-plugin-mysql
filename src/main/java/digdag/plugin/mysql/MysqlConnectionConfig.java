package io.digdag.plugin.mysql;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.spi.SecretProvider;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnectionConfig;
import io.digdag.util.DurationParam;
import org.immutables.value.Value;

import java.time.Duration;
import java.util.Properties;

@Value.Immutable
public abstract class MysqlConnectionConfig extends AbstractJdbcConnectionConfig {
    public abstract Optional<String> schema();

    @VisibleForTesting
    public static MysqlConnectionConfig configure(SecretProvider secrets, Config params) {
        return ImmutableMysqlConnectionConfig.builder()
            .host(secrets.getSecretOptional("host").or(() -> params.get("host", String.class)))
            .port(secrets.getSecretOptional("port").transform(Integer::parseInt).or(() -> params.get("port", int.class, 3306)))
            .password(secrets.getSecretOptional("password"))
            .database(secrets.getSecretOptional("database").or(() -> params.get("database", String.class)))
            // table setting is needed
            // connection timeout settings are needed
            .build();
    }

    @Override
    public String jdbcDriverName() {
        return "org.mysql.Driver";
    }

    @Override
    public String jdbcProtocolName() {
        return "mysql";
    }

    @Override
    public Properties buildProperties() {
        Properties props = new Properties();

        props.setProperty("user", user());
        if (password().isPresent()) {
            props.setProperty("password", password().get());
        }
        // table setting is needed
        // connection timeout settings are needed
        props.setProperty("applicationName", "digdag");
        return props;
    }

    @Override
    public String toString() {
        return url();
    }
}

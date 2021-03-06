package io.digdag.plugin.mysql;

import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.jdbc.AbstractJdbcJobOperator;

public class MysqlOperatorFactory implements OperatorFactory {
    private static final String OPERATOR_TYPE = "mysql";
    private final Config systemConfig;
    private final TemplateEngine templateEngine;

    @Inject
    public MysqlOperatorFactory(Config systemConfig, TemplateEngine templateEngine) {
        this.systemConfig = systemConfig;
        this.templateEngine = templateEngine;
    }

    @Override
    public String getType() {
        return OPERATOR_TYPE;
    }

    @Override
    public MysqlOperator newOperator(OperatorContext context) {
        return new MysqlOperator(systemConfig, context, templateEngine);
    }

    static class MysqlOperator extends AbstractJdbcJobOperator<MysqlConnectionConfig> {
        MysqlOperator(Config systemConfig, OperatorContext context, TemplateEngine templateEngine) {
            super(systemConfig, context, templateEngine);
        }

        @Override
        protected MysqlConnectionConfig configure(SecretProvider secrets, Config params) {
            return MysqlConnectionConfig.configure(secrets, params);
        }

        @Override
        protected MysqlConnection connect(MysqlConnectionConfig connectionConfig) {
            return MysqlConnection.open(connectionConfig);
        }

        @Override
        protected String type() {
            return OPERATOR_TYPE;
        }

        @Override
        protected SecretProvider getSecretsForConnectionConfig() {
            return context.getSecrets().getSecrets(type());
        }
    }
}

package io.digdag.plugin.mysql;

import static java.nio.charset.StandardCharsets.UTF_8;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorContext;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.OperatorProvider;
import io.digdag.spi.Plugin;
import io.digdag.spi.TemplateEngine;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

public class MysqlPlugin implements Plugin {
    @Override
    public <T> Class<? extends T> getServiceProvider(Class<T> type) {
        if (type == OperatorProvider.class) {
            return MysqlOperatorProvider.class.asSubclass(type);
        } else {
            return null;
        }
    }

    public static class MysqlOperatorProvider implements OperatorProvider {
        @Inject
        protected TemplateEngine templateEngine;
        protected Config systemConfig;

        @Override
        public List<OperatorFactory> get() {
            return Arrays.asList(new MysqlOperatorFactory(systemConfig, templateEngine));
        }
    }
}

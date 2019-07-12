package org.jasmine.stream.config;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

public class FlinkConfiguration {
    private final static String PROPERTIES_FILENAME = "application.properties";
    private static FlinkConfiguration instance;
    private ParameterTool parameterTool;

    private FlinkConfiguration() {
        this.parameterTool = initializeParams();
    }

    private static FlinkConfiguration getInstance() {
        return (instance == null ? (instance = new FlinkConfiguration()) : instance);
    }

    public static ParameterTool getParameters() {
        return getInstance().getParameterTool();
    }

    public static ParameterTool getParametersWithArgs(String[] args) {
        getInstance().parameterTool = getInstance().parameterTool.mergeWith(ParameterTool.fromArgs(args));

        return getInstance().getParameterTool();
    }

    public static ParameterTool getParametersRefreshing() {
        getInstance().parameterTool = getInstance().parameterTool.mergeWith(getInstance().initializeParams());

        return getInstance().getParameterTool();
    }

    private ParameterTool initializeParams() {
        ParameterTool parameterTool = ParameterTool.fromSystemProperties();

        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILENAME);
            parameterTool = parameterTool.mergeWith(ParameterTool.fromPropertiesFile(is));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return parameterTool;
    }

    private ParameterTool getParameterTool() {
        return this.parameterTool;
    }
}

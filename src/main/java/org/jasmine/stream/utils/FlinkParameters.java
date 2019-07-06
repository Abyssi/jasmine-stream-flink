package org.jasmine.stream.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

public class FlinkParameters {
    private final static String PROPERTIES_FILENAME = "application.properties";
    private static FlinkParameters instance;
    private ParameterTool parameterTool;

    private FlinkParameters() {
        this.parameterTool = initializeParams();
    }

    private static FlinkParameters getInstance() {
        return (instance == null ? (instance = new FlinkParameters()) : instance);
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

package org.jasmine.stream.operators;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.java.functions.KeySelector;

public abstract class RichKeySelector<IN, OUT> extends AbstractRichFunction implements KeySelector<IN, OUT> {
}

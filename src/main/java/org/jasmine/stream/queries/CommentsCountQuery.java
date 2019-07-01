package org.jasmine.stream.queries;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.jasmine.stream.models.CommentHourlyCount;
import org.jasmine.stream.models.CommentInfo;
import org.jasmine.stream.models.CommentType;
import org.jasmine.stream.operators.CollectorAggregateFunction;
import org.jasmine.stream.operators.CounterAggregateFunction;
import org.jasmine.stream.operators.TimestampEnrichProcessAllWindowFunction;
import org.jasmine.stream.utils.DateUtils;

import java.util.Calendar;
import java.util.HashMap;

public class CommentsCountQuery {
    public static DataStream<CommentHourlyCount> run(DataStream<CommentInfo> inputStream, Time window) {
        return inputStream
                .filter(item -> item.getCommentType() == CommentType.COMMENT)
                .map(item -> {
                    Calendar calendar = DateUtils.parseCalendar(item.getCreateDate());
                    return (int) Math.floor(calendar.get(Calendar.HOUR_OF_DAY) / 2.0);
                })
                .keyBy(s -> s)
                .timeWindow(window)
                .aggregate(new CounterAggregateFunction<>())
                .timeWindowAll(window)
                .aggregate(new CollectorAggregateFunction<>(), new TimestampEnrichProcessAllWindowFunction<>())
                .map(item -> {
                    HashMap<Integer, Long> value = item.getElement();
                    return new CommentHourlyCount(item.getTimestamp(), value.get(0), value.get(1), value.get(2), value.get(3), value.get(4), value.get(5), value.get(6), value.get(7), value.get(8), value.get(9), value.get(10), value.get(11));
                });
    }
}

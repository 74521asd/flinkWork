package com.wanshen.job.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class MyRetractStreamTableSink implements RetractStreamTableSink<Row> {
    @Override
    public TypeInformation<Row> getRecordType() {
        return null;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        dataStream.map(t->{
            Boolean f0 = t.f0;
            if (f0){
                System.out.println("做delete");
                t.f1.setKind(RowKind.DELETE);
            }else {
                t.f1.setKind(RowKind.INSERT);
            }
            System.out.println("做insert");
            return t.f1;
        });
        return null;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        return null;
    }
}

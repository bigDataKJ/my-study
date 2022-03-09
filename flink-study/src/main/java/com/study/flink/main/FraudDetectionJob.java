package com.study.flink.main;

import com.study.flink.processFunction.FraudDetector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * @Description: 基于DataStream API实现的欺诈检测
 * @Author: kangjiang
 * @Date: 2022/3/9 15:22
 * @Version: 1.0
 */
public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Transaction> transactions = env.addSource(new TransactionSource()).name("transactions");
        SingleOutputStreamOperator<Alert> alerts = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");
        alerts.addSink(new AlertSink()).name("send-alerts");
        env.execute("Fraud Detection");
    }
}

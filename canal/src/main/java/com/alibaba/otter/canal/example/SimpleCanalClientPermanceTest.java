package com.alibaba.otter.canal.example;

import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;
import com.alibaba.otter.canal.protocol.Message;

/**
 * 172.20.0.207 dev-dw1.hbfintech.com dev-dw1
 * 172.20.0.203 dev-dw2.hbfintech.com dev-dw2
 * 172.20.0.204 dev-dw3.hbfintech.com dev-dw3
 * 172.20.0.205 dev-dw4.hbfintech.com dev-dw4
 * 172.20.0.206 dev-dw5.hbfintech.com dev-dw5
 */
public class SimpleCanalClientPermanceTest {

    public static void main(String args[]) {
        String destination = "example";
        String ip = "172.20.0.203";
        int batchSize = 1024;
        int count = 0;
        int sum = 0;
        int perSum = 0;
        long start = System.currentTimeMillis();
        long end = 0;
        final ArrayBlockingQueue<Long> queue = new ArrayBlockingQueue<Long>(100);
        try {
            final CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111),
                destination,
                "canal",
                "canal");

            Thread ackThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    while (true) {
                        try {
                            long batchId = queue.take();
                            connector.ack(batchId);
                        } catch (InterruptedException e) {
                        }
                    }
                }
            });
            ackThread.start();

            ((SimpleCanalConnector) connector).setLazyParseEntry(true);
            connector.connect();
            connector.subscribe();
            while (true) {
                Message message = connector.getWithoutAck(batchSize, 100L, TimeUnit.MILLISECONDS);
                long batchId = message.getId();
                int size = message.getRawEntries().size();
                sum += size;
                perSum += size;
                count++;
                queue.add(batchId);
                if (count % 10 == 0) {
                    end = System.currentTimeMillis();
                    if (end - start != 0) {
                        long tps = (perSum * 1000) / (end - start);
                        System.out.println(" total : " + sum + " , current : " + perSum + " , cost : " + (end - start)
                                           + " , tps : " + tps);
                        start = end;
                        perSum = 0;
                    }
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

}

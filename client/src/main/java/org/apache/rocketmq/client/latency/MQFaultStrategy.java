/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    private boolean sendLatencyFaultEnable = false;

    // 最大延迟时间数值, 通过 下标 回去获取 notAvailableDuration
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * #1 是否开启消息失败延迟规避机制, false 直接从 topic 的所有队列中选择下一个，而不考虑该消息队列是否可用 (比如Broker挂掉）
     * #2 start -- end 这里使用了本地线程变量 ThreadLocal 保存上一次发送的消息队列下标
     * #3 判断当前的消息队列是否可用
     * #2 -- #3 一旦一个 MessageQueue 符合条件，即刻返回, 如果不可用, 则会进入到 #4 (标记为不可用，并不代表真的不可用，Broker 是可以在故障期间被运营管理人员进行恢复的，比如重启)
     * #4 #5 根据 Broker 的 startTimestart 进行一个排序，值越小，排前面，然后再选择一个, 返回（此时不能保证一定可用，会抛出异常，如果消息发送方式是同步调用，则有重试机制）
     * @param tpInfo
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        //  启用 Broker 故障延迟机制
        if (this.sendLatencyFaultEnable) { // #1
            try {
                int index = tpInfo.getSendWhichQueue().getAndIncrement(); // #2 start
                // 根据对消息队列进行轮询获取一个消息队列
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos); // #2 end
                    // 验证该消息队列是否可用，latencyFaultTolerance.isAvailable(mq.getBrokerName()) 是关键
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName())) { // #3
                        if (null == lastBrokerName || mq.getBrokerName().equals(lastBrokerName))
                            return mq;
                    }
                }

                // 尝试从规避的 Broker 中选择一个可用的 Broker ，如果没有找到，将返回 null
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast(); // #4
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker); // #5 start
                if (writeQueueNums > 0) {
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().getAndIncrement() % writeQueueNums);
                    }
                    return mq;
                } else {
                    // 表明故障已经恢复
                    latencyFaultTolerance.remove(notBestBroker);    // #5 end
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            return tpInfo.selectOneMessageQueue();
        }

        // 默认不启用 Broker 故障延迟机制, lastBrokerName 上 一 次选择的执行发送消息失败的 Broker， 下次发送规避上次发送失败的 broker
        return tpInfo.selectOneMessageQueue(lastBrokerName); // #6
    }


    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        if (this.sendLatencyFaultEnable) {
            // duration 不可用时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}

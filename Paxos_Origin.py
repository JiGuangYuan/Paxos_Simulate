# -*- coding=utf-8 -*-
# Created on 2017年2月15日
# @author: JiGuang Yuan
"""

该模拟程序主要用来模拟分布式计算中Paxos算法的应用
系统分为四个部分：
Leader：决议的提出者，负责提出决议并等待各个Leaner表决
Proposer：竞争Leader，向acceptor发出选票
Acceptor: 议案的表决者，负责接收议案并进行表决
learner: 议案的学习者，负责执行leader提出的议案

"""

import random
import threading
import time
from multiprocessing import Queue
from queue import Empty

mutex = threading.Lock()

# 一次接受返回值等待的时间
OVER_TIME = 5
# 网络丢包率
PACKET_LOSS = 20
# learner忙碌的概率
BUSS = 20
# proposers的数量
proposers_num = 3
# acceptors的数量
acceptors_num = 5
# learners的数量
learners_num = 5
# 发送消息数
send_msg_num = 0
# 提交失败数
fail_msg_num = 0
# 模拟Acceptor宕机的个数
crash_num = 2
crash_list = random.sample(range(acceptors_num), crash_num)


def print_str(string):
    """
    打印信息的函数
    避免多线程打印打乱的情况
    :param string:  需要打印的字符串
    :return:
    """
    mutex.acquire()
    print(string)
    mutex.release()


def network_delay():
    """
    模拟网络传输延迟
    :return: 
    """
    # 200ms~250ms 延迟
    time.sleep(1 / random.randint(4, 5))


class Proposer(threading.Thread):
    """
    Paxos Proposer决议发起者
    议案的提出者，负责提出议案并等待各个接收者的表决
    """

    def __init__(self, th_name, queue_from_acceptor, queue_to_acceptors, proposer_id):
        """
        构造函数
        :param th_name: Proposer名称
        :param queue_from_acceptor: Proposer接收消息队列
        :param queue_to_acceptors: Proposer和acceptor通讯的消息队列
        :param proposer_id: Proposer ID号
        """
        threading.Thread.__init__(self, name=th_name)
        self.queue_recv = queue_from_acceptor
        self.queue_send = queue_to_acceptors
        self.id = proposer_id  # proposer的id
        self.promise_num = 0  # 承若的acceptor计数
        self.reject_num = 0  # 拒绝的acceptor计数
        self.ack_num = 0  # 同意的acceptor计数
        self.nack_num = 0  # 不同意的acceptor计数
        self.first_stage = False  # proposer第一阶段标志
        self.fail_list = []  # 超时失效的消息队列
        self.v_num = 100 + self.id  # 申请的编号
        self.value = th_name + "申请成为leader"  # 申请的内容
        self.acceptors = range(acceptors_num)  # acceptors的列表
        self.var = {}  # 发送的消息
        self.time_start = 0

    def run(self):
        global send_msg_num
        global fail_msg_num
        # 给自己发送一个start信号
        start_sig = {"type": "start"}
        self.queue_recv.put(start_sig)
        send_msg_num += 1
        # 循环接收从acceptor过来的消息
        while True:
            try:
                # 阻塞调用,至多阻塞1秒
                var = self.queue_recv.get(True, 1)
                # 模拟网络传输延迟
                network_delay()
                # 接收到消息，准备处理
                self.process_msg(var)
            except Empty:
                # 本次投票结束了
                if time.time() - self.time_start > OVER_TIME:
                    if self.first_stage is True:
                        print_str(
                            self.value + "：<< 第一阶段 >>投票结束，Promise:" + str(self.promise_num) + " ,Reject:" + str(
                                self.reject_num))
                        self.first_stage = False
                        # 判断投票结果
                        if self.reject_num > 0 or self.nack_num > 0:
                            # 如果有一个拒绝则被否决
                            print_str(
                                "-------------    " + self.name + "的申请" + self.value + "被否决，重新申请    ---------------")
                            self.re_send()
                        # 如果超过半数promise则向acceptor发送decide信号
                        elif self.promise_num > round(len(self.acceptors) / 2):
                            self.time_start = time.time()
                            for acceptor in self.acceptors:
                                # 提出申请，有概率发送失败
                                if random.randrange(100) < (100 - PACKET_LOSS):
                                    self.var = {
                                        "status": "start",
                                        "type": "decide",
                                        "V_num": self.v_num,
                                        "Value": self.value,
                                        "proposer_id": self.id
                                    }
                                    print_str("编号：" + str(self.v_num) + " ，决议：" + self.var["Value"])
                                    self.queue_send[acceptor].put(self.var)
                                    send_msg_num += 1
                                else:
                                    print_str(self.name + "   >>>>>    发送决定失败")
                                    fail_msg_num += 1
                        else:
                            # 网络原因获取回应失败，重新开始申请
                            self.re_send()
                    else:
                        print_str(
                            self.value + "：<< 第二阶段 >>投票结束，ack:" + str(self.ack_num) + " ,nack:" + str(self.nack_num))
                        # 判断投票结果
                        if self.nack_num > 0:
                            # 如果有一个拒绝则被否决
                            print_str(
                                "-------------    " + self.name + "的申请" + self.value + "被否决，重新申请    ---------------")
                            self.re_send()
                        # 如果超过半数ack 则说明申请已经被acceptor同意
                        elif self.ack_num > round(len(self.acceptors) / 2):
                            print_str(
                                ">>>>>>>>>>>>>>>    " + self.value + "被同意，完成投票过程    <<<<<<<<<<<<<<<")
                            print_str("-------------------------- " + self.name + "：成为了Leader-----------------------")
                            # Proposer竞争为leader成功，启动leader进程
                            ld = Leader("Leader", q_to_leader, q_to_learners)
                            ld.start()
                            # (改进) proposer直接告之其它Proposer停止申请，设置proposer全局变量
                            for acceptor in self.acceptors:
                                self.var = {"status": "stop", "proposer_id": self.id}
                                print_str("结束选举,清空队列,发出结束信号")
                                while not self.queue_send[acceptor].empty():
                                    self.queue_send[acceptor].get()
                                self.queue_send[acceptor].put(self.var)
                                send_msg_num += 1
                        else:
                            # 网络原因获取回应失败，重新开始申请
                            self.re_send()

    def re_send(self):
        self.promise_num = 0
        self.reject_num = 0
        self.ack_num = 0
        self.nack_num = 0
        self.send_propose()

    def process_msg(self, var):
        """
        处理报文
        :param var: 消息报文
        :return:
        """
        # 如果是启动命令，启动程序
        if var["type"] == "start":
            self.send_propose()
        elif var["type"] == "stop":
            while True:
                continue
        # 如果是acceptor过来的报文，解析报文
        elif var["type"] == "accepting":
            if time.time() - self.time_start < OVER_TIME:
                if var["result"] == "promise":
                    self.promise_num += 1
                elif var["result"] == "reject":
                    # 拒绝的申请编号+1
                    self.reject_num += 1
                    self.v_num = var["max_val"] + 1
            else:
                # 超时接收,则丢弃
                print_str("消息报文超时失效，丢弃...")
                self.fail_list.append(var["acceptor_id"])
        elif var["type"] == "deciding":
            if time.time() - self.time_start < OVER_TIME:
                if var["result"] == "ack":
                    self.ack_num += 1
                elif var["result"] == "nack":
                    self.nack_num += 1

    def send_propose(self):
        """
        发送一次申请给所有acceptor
        :return:
        """
        global send_msg_num
        global fail_msg_num
        self.time_start = time.time()
        self.first_stage = True
        # 模拟发送时延50ms-1000ms
        time.sleep(1 / random.randrange(1, 20))
        # 申请自身为leader
        print_str(self.value)
        for acceptor in self.acceptors:
            # 提出申请，有概率发送失败
            if random.randrange(100) < (100 - PACKET_LOSS):
                self.var = {
                    "status": "start",
                    "type": "proposing",
                    "V_num": self.v_num,
                    "Value": self.value,
                    "proposer_id": self.id
                }
                print_str(
                    self.name + "   >>>>>    编号：" + str(self.v_num) + " ，决议：" +
                    str(self.var["Value"]) + " ,日期：" + time.strftime("%Y-%m-%d %H:%M:%S",
                                                                     time.localtime(self.time_start)))
                self.queue_send[acceptor].put(self.var)
                send_msg_num += 1
            else:
                print_str(self.name + "   >>>>>    发送申请失败")
                fail_msg_num += 1


class Acceptor(threading.Thread):
    """
    paxos 申请表决者acceptor
    负责接收proposer的申请进行表决
    """

    def __init__(self, th_name, queue_from_proposer, queue_to_proposers, acceptor_id):
        """
        构造函数
        :param th_name: Acceptor的名称
        :param queue_from_proposer: 从proposer收到的消息队列
        :param queue_to_proposers: 向proposer发送的消息队列
        :param acceptor_id: acceptor的ID
        """
        threading.Thread.__init__(self, name=th_name)
        self.queue_recv = queue_from_proposer
        self.queue_send = queue_to_proposers
        self.id = acceptor_id
        self.proposers = range(proposers_num)
        self.isStart = True
        self.values = {
            "last": 0,  # 最后一次表决的申请编号
            "value": "",  # 最后一次表决的申请的内容
            "max": 0  # 承诺的最低表决申请编号
        }

    def run(self):
        global send_msg_num
        global fail_msg_num
        # 模拟宕机状态
        while self.id not in crash_list:
            try:
                var = self.queue_recv.get(False)
                # 模拟网络传输延迟
                network_delay()
                rsp = self.process_propose(var)
                if self.isStart:
                    # 有概率发送失败
                    if random.randrange(100) < (100 - PACKET_LOSS):
                        self.queue_send[var["proposer_id"]].put(rsp)
                        send_msg_num += 1
                    else:
                        print_str(self.name + "   >>>>>    发送审批失败")
                        fail_msg_num += 1
                else:
                    for proposer in self.proposers:
                        self.queue_send[proposer].put(rsp)
                        send_msg_num += 1

            except Empty:
                continue

    def process_propose(self, value):
        """
         处理申请提出者提出的申请
        :param value: 决议的值
        :return: 响应报文
        """
        res = {}
        if value["status"] == "stop":
            self.isStart = False
            res = {"type": "stop"}
        # 第一阶段申请阶段
        elif value["type"] == "proposing":
            # 如果从来没接收过申请，更新自身申请
            if self.values["max"] == 0 and self.values["last"] == 0:
                self.values["max"] = value["V_num"]
                self.values["last"] = value["V_num"]
                self.values["value"] = value["Value"]
                res = {
                    "type": "accepting",
                    "result": "promise",
                    "last": 0,
                    "value": self.values["value"],
                    "acceptor_id": self.id
                }
            else:
                # 如果接收的申请编号大于承诺最低表决的申请编号，同意并告知之前表决结果
                if self.values["max"] < value["V_num"]:
                    self.values["max"] = value["V_num"]
                    res = {
                        "type": "accepting",
                        "result": "promise",
                        "last": self.values["last"],
                        "value": self.values["value"],
                        "acceptor_id": self.id
                    }
                else:
                    # 如果收到的申请小于承诺最低表决的申请，直接拒绝
                    res = {
                        "max_val": self.values["max"],
                        "type": "accepting",
                        "result": "reject",
                        "last": self.values["last"],
                        "value": self.values["value"],
                        "acceptor_id": self.id
                    }
        # 第二阶段确定阶段
        elif value["type"] == "decide":
            if self.values["max"] == value["V_num"]:
                # 如果收到的申请编号等于承诺最低表决的申请编号，完全同意申请，表决结束
                self.values["last"] = value["V_num"]
                self.values["value"] = value["Value"]
                res = {
                    "type": "deciding",
                    "result": "ack",
                    "last": self.values["last"],
                    "value": self.values["value"],
                    "acceptor_id": self.id
                }
            else:
                # 如果收到的申请小于承诺最低表决的申请，直接拒绝
                res = {
                    "max_val": self.values["max"],
                    "type": "deciding",
                    "result": "nack",
                    "last": self.values["last"],
                    "value": self.values["value"],
                    "acceptor_id": self.id
                }
        return res


class Leader(threading.Thread):
    """
    Paxos Leader 决议提出者
    决议的提出者，负责提出决议并等待各个Leaner表决
    """

    def __init__(self, th_name, queue_from_learner, queue_to_learner):
        """
        构造函数
        :param th_name: Leader名称
        :param queue_from_learner: Leader从learner接收请求的队列
        :param queue_to_learner: 和learner通讯的消息队列
        """

        threading.Thread.__init__(self, name=th_name)
        self.queue_recv = queue_from_learner
        self.queue_send = queue_to_learner
        self.recv_ready_num = 0
        self.recv_ack_num = 0
        self.time_start = 0
        self.prepare_num = 0
        self.values = ["[决议：A]",
                       "[决议：B]",
                       "[决议：C]",
                       "[决议：D]",
                       "[决议：E]",
                       "[决议：F]",
                       "[决议：G]",
                       "[决议：H]"]
        self.index = 0
        self.value_num = 1
        self.isPrepare = True  # 是否为Prepare阶段
        self.isStarting = True  # 是否为开始状态

    def run(self):
        global send_msg_num, fail_msg_num
        self.send_prepare()
        while True:
            # 接收learner回复的状态信息
            try:
                var = self.queue_recv.get(False)
                # 模拟网络传输延迟
                network_delay()
                # 获取ready信息
                if var["type"] == "ready":
                    if time.time() - self.time_start < OVER_TIME and var["prepare_num"] == self.prepare_num:
                        self.recv_ready_num += 1
                        if self.recv_ready_num == learners_num:
                            # 第二阶段发送 commit信号
                            self.send_commit()
                    else:
                        # 超时接收,则丢弃
                        print_str("获取ready信号超时失效，丢弃...")
                # 获取到reject信号
                elif var["type"] == "reject":
                    print_str("接受到reject信号重新计数")
                    # 重置计数
                    self.recv_ready_num = 0
                    # 并且重新进行prepare请求
                    self.send_prepare()
                # 获取ack成功
                elif var["type"] == "ack":
                    self.isPrepare = False
                    if time.time() - self.time_start < OVER_TIME:
                        self.recv_ack_num += 1
                        if self.recv_ack_num == learners_num:
                            # 如果接收全部ack回应，则表示成功
                            print_str(">>>>>>>>这次分布式一致性决议,完成<<<<<<<<<")
                            end_time = time.time()
                            print_str("耗时:" + str(round(end_time - start_time)) + "秒")
                            print_str("消息传递数：" + str(send_msg_num))
                            print_str("提交消息无效数:" + str(fail_msg_num))
                            self.isStarting = False
                    else:
                        # 超时接收,则丢弃
                        print_str("获取ack信号超时失效，丢弃...")

            except Empty:
                # 没有接受到回应
                if self.isStarting and time.time() - self.time_start > OVER_TIME:
                    if self.isPrepare:
                        # 重置计数
                        self.recv_ready_num = 0
                        # 并且重新进行prepare请求
                        self.send_prepare()
                    else:
                        # 重置计数
                        self.recv_ack_num = 0
                        # 如果没有接收到全部ack回应，则重复commit
                        self.send_commit()
                continue

    def send_prepare(self):
        global send_msg_num
        self.prepare_num += 0
        req = {
            "type": "prepare",
            "prepare_num": self.prepare_num
        }
        for n in range(0, learners_num):
            # 发送prepare信号给learner
            self.queue_send[n].put(req)
            print_str("发送 prepare 信号")
            send_msg_num += 1
            self.time_start = time.time()
        self.index = random.randrange(8)
        self.value_num += 1

    def send_commit(self):
        global send_msg_num
        req = {
            "type": "commit",
            "value": self.values[self.index],  # 议案内容
            "value_num": self.value_num,  # 议案编号
        }
        # 发送commit信号给learner
        for n in range(learners_num):
            self.queue_send[n].put(req)
            send_msg_num += 1
            print_str("Leader >>>>> 发送 commit 信号")
        self.time_start = time.time()


class Learner(threading.Thread):
    """
    Learner负责响应Leader 的决议，并执行决议
    """

    def __init__(self, th_name, queue_from_leader, queue_to_leader, learner_id):
        """
        构造函数
        :param th_name:  Learner的名称
        :param queue_from_leader: 从leader传来的消息队列
        :param queue_to_leader: 向leader发送的消息队列
        :param learner_id: Learner ID
        :return:
        """
        threading.Thread.__init__(self, name=th_name)
        self.queue_recv = queue_from_leader
        self.queue_send = queue_to_leader
        self.id = learner_id

    def run(self):
        global send_msg_num
        global fail_msg_num
        while True:
            try:
                rsp = {}
                var = self.queue_recv.get(False)
                # 模拟网络传输延迟
                network_delay()
                if var["type"] == "prepare":
                    if random.randrange(100) < (100 - BUSS):
                        # 发送ready响应信号回去
                        rsp = {
                            "type": "ready",
                            "prepare_num": var["prepare_num"]
                        }
                    else:
                        # 20% 的概率自身正忙
                        rsp = {
                            "type": "reject",
                            "prepare_num": var["prepare_num"]
                        }
                elif var["type"] == "commit":
                    # 发送ack响应信号回去
                    rsp = {"type": "ack"}
                    # 有概率发送失败
                if random.randrange(100) < (100 - PACKET_LOSS):
                    self.queue_send.put(rsp)
                    print_str(self.name + " 发送响应" + str(rsp))
                    send_msg_num += 1
                else:
                    print_str(self.name + " 发送响应失败" + str(rsp))
                    fail_msg_num += 1
            except Empty:
                continue


if __name__ == '__main__':
    q_to_proposers = []  # proposer通讯的消息队列
    q_to_acceptors = []  # acceptor通讯的消息队列
    q_to_learners = []  # learner通讯的消息队列
    q_to_leader = Queue()  # leader通讯的消息队列
    start_time = time.time()
    print("----分布式一致性协议系统开始----")
    for i in range(proposers_num):
        q_to_proposers.append(Queue())
        proposer_th = Proposer("proposer'" + str(i) + "'", q_to_proposers[i], q_to_acceptors, i)
        proposer_th.setDaemon(True)
        proposer_th.start()

    for i in range(acceptors_num):
        q_to_acceptors.append(Queue())
        acceptor_th = Acceptor("Acceptor'" + str(i) + "'", q_to_acceptors[i], q_to_proposers, i)
        acceptor_th.setDaemon(True)
        acceptor_th.start()

    # learner进程的开启
    for i in range(learners_num):
        q_to_learners.append(Queue())
        learner_th = Learner("Learner'" + str(i) + "'", q_to_learners[i], q_to_leader, i)
        learner_th.setDaemon(True)
        learner_th.start()

# -*- coding=utf-8 -*-
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
# 网络发送时延
SEND_DELAY = 0
# proposers的数量
proposers_num = 5
# acceptors的数量
acceptors_num = 10
# learners的数量
learners_num = 10
# 发送消息数
send_msg_num = 0
# 提交失败数
fail_msg_num = 0
# 模拟宕机的个数
crash_num = 0
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
    time.sleep(1 / random.randrange(4, 5))


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
        self.reject_num = 0  # 拒绝的acceptor计数
        self.promise_num = 0  # 保证的acceptor计数
        self.ack_num = 0  # 同意的acceptor计数
        self.start_propose = False  # proposer开始标志
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
                # 投票结束了
                if self.start_propose is True and time.time() - self.time_start > OVER_TIME:
                    print_str(self.value + "投票结束，Promise:" + str(
                        self.promise_num) + " ,Reject:" + str(
                        self.reject_num) + " ,Ack: " + str(self.ack_num))
                    self.start_propose = False
                    # 判断投票结果
                    if self.reject_num > 0:
                        # 如果有一个拒绝则被否决
                        print_str(
                            "-------------    " + self.name + "的申请" + self.value + "被否决，重新申请    ---------------")
                        self.promise_num = 0
                        self.reject_num = 0
                        self.ack_num = 0
                        self.send_propose()
                        continue
                        # if self.chosen > len(self.acceptors) / 2:
                    elif self.ack_num > round(len(self.acceptors) / 2):
                        # 如果超过半数chosen则被同意
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
                    elif (self.promise_num > 0 or (
                                        len(self.acceptors) > self.ack_num > 0 and self.reject_num == 0) or (
                                        self.promise_num == 0 and self.ack_num == 0 and self.reject_num == 0)):
                        # 网络原因获取回应失败，重新开始申请
                        self.promise_num = 0
                        self.reject_num = 0
                        self.ack_num = 0
                        self.send_propose()
                continue

    def process_msg(self, var):
        """
        处理报文
        :param var: 消息报文
        :return:
        """
        global send_msg_num
        global fail_msg_num
        # 如果是启动命令，启动程序
        if var["type"] == "start":
            self.send_propose()
        elif var["type"] == "stop":
            while True:
                continue
        # 如果是acceptor过来的报文，解析报文
        elif var["type"] == "accepting":
            if time.time() - self.time_start < OVER_TIME:
                if var["result"] == "reject":
                    # 拒绝的申请编号+1
                    self.reject_num += 1
                    self.v_num = var["max_val"] + 1
                elif var["result"] == "promise":
                    self.promise_num += 1
                    """
                    # 修改决议为acceptor建议的决议
                    self.value = var["value"]
                    self.var = {
                        "status": "start",
                        "type": "proposing",
                        "V_num": self.v_num,
                        "Value": var["value"],
                        "proposer_id": self.num
                    }

                    """
                elif var["result"] == "ack":
                    self.ack_num += 1
            else:
                # 超时接收,则丢弃
                print_str("消息报文超时失效，丢弃...")
                self.fail_list.append(var["acceptor_id"])
                fail_msg_num += 1

    def send_propose(self):
        """
        发送一次申请给所有acceptor
        :return:
        """
        global send_msg_num
        global fail_msg_num
        self.time_start = time.time()
        self.start_propose = True
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
                    "proposer_id": self.id,
                    "time": self.time_start
                }
                print_str(
                    self.name + "   >>>>>    " + "类型：" + self.var["type"] + " ，编号：" + str(self.v_num) + " ，决议：" +
                    self.var["Value"] + " ,日期：" + time.strftime("%Y-%m-%d %H:%M:%S",
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
            "max": 0}  # 承诺的最低表决申请编号

    def run(self):
        global send_msg_num
        global fail_msg_num
        while True:
            try:
                var = self.queue_recv.get(False)
                # 模拟网络传输延迟
                network_delay()
                rsp = self.process_propose(var)
                if self.isStart:
                    self.queue_send[var["proposer_id"]].put(rsp)
                    send_msg_num += 1
                else:
                    for proposer in self.proposers:
                        self.queue_send[proposer].put(rsp)
                        send_msg_num += 1
                '''
                # 有概率发送失败
                if random.randrange(100) < (100 - PACKET_LOSS):
                    self.queue_send[var["proposer_id"]].put(rsp)
                else:
                    print_str(self.name + "   >>>>>    发送审批失败")
                '''
            except Empty:
                continue

    def process_propose(self, value):
        """
         处理申请提出者提出的申请
        :param value: 决议的值
        :return: 响应报文
        """
        if value["status"] == "stop":
            self.isStart = False
            res = {"type": "stop"}
            # 如果从来没接收过申请，更新自身申请
        elif self.values["max"] == 0 and self.values["last"] == 0:
            self.values["max"] = value["V_num"]
            self.values["last"] = value["V_num"]
            self.values["value"] = value["Value"]
            res = {
                "type": "accepting",
                "result": "promise",
                "last": 0,
                "value": self.values["value"],
                "acceptor_id": self.id,
                "time": value["time"]}
        else:
            # 如果接收的申请编号大于承诺最低表决的申请编号，同意并告知之前表决结果
            if self.values["max"] < value["V_num"]:
                self.values["max"] = value["V_num"]
                res = {
                    "type": "accepting",
                    "result": "promise",
                    "last": self.values["last"],
                    "value": self.values["value"],
                    "acceptor_id": self.id,
                    "time": value["time"]}
            elif self.values["max"] == value["V_num"]:
                # 如果收到的申请编号等于承诺最低表决的申请编号，完全同意申请，表决结束
                self.values["last"] = value["V_num"]
                self.values["value"] = value["Value"]
                res = {
                    "type": "accepting",
                    "result": "ack",
                    "last": self.values["last"],
                    "value": self.values["value"],
                    "acceptor_id": self.id,
                    "time": value["time"]
                }
            else:
                # 如果收到的申请小于承诺最低表决的申请，直接拒绝
                res = {
                    "max_val": self.values["max"],
                    "type": "accepting",
                    "result": "reject",
                    "last": self.values["last"],
                    "value": self.values["value"],
                    "acceptor_id": self.id,
                    "time": value["time"]
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
        self.fail_list = []
        self.values = ["[决议：A]",
                       "[决议：B]",
                       "[决议：C]",
                       "[决议：D]",
                       "[决议：E]",
                       "[决议：F]",
                       "[决议：G]",
                       "[决议：H]"]
        self.value_num = 1

    def run(self):
        global send_msg_num
        global fail_msg_num
        rsp = {"type": "prepare"}
        for n in range(0, learners_num):
            # 发送prepare信号给learner
            self.queue_send[n].put(rsp)
            print_str("发送 prepare 信号")
            send_msg_num += 1
            self.time_start = time.time()
        index = random.randrange(8)
        while True:

            # 接收learner回复的状态信息
            try:
                var = self.queue_recv.get(False)
                # 模拟网络传输延迟
                network_delay()
                # 获取ready信息
                if var["type"] == "ready":
                    rsp = {
                        "type": "commit",
                        "value": self.values[index],  # 议案内容
                        "value_num": self.value_num,  # 议案编号
                    }
                    if time.time() - self.time_start < OVER_TIME:
                        self.recv_ready_num += 1
                        if self.recv_ack_num == learners_num:
                            # 发送commit信号给learner
                            for n in range(learners_num):
                                self.queue_send[n].put(rsp)
                                send_msg_num += 1
                                print_str("Leader >>>>> 发送 commit 信号")
                            self.time_start = time.time()
                    else:
                        # 超时接收,则丢弃
                        print_str("获取ready信号超时失效，丢弃...")
                        self.fail_list.append(var["learner_id"])
                        fail_msg_num += 1
                # （改进） 如果没有接收到learner的全部回应，则立即重新发送prepare请求
                # 获取ack成功
                elif var["type"] == "ack":
                    if time.time() - self.time_start < OVER_TIME:
                        self.recv_ack_num += 1
                        if self.recv_ack_num == learners_num:
                            # 如果接收全部的learner的ack回应，则表示成功
                            print_str(">>>>>>>>这次分布式一致性决议,完成<<<<<<<<<")
                            end_time = time.time()
                            print_str("耗时:" + str(round(end_time - start_time)) + "秒")
                            print_str("消息传递数：" + str(send_msg_num))
                            print_str("提交消息无效数:" + str(fail_msg_num))
                            # 如果没有接收到全部learner的ack回应，则重复此过程
                    else:
                        # 超时接收,则丢弃
                        print_str("获取ack信号超时失效，丢弃...")
                        fail_msg_num += 1
            except Empty:
                continue


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
        # 模拟宕机状态，如果
        while self.id not in crash_list:
            try:
                rsp = {}
                var = self.queue_recv.get(False)
                # 模拟网络传输延迟
                network_delay()
                print_str(var)
                if var["type"] == "prepare":
                    # 接收到prepare信息，则发送ready信号回去
                    rsp = {"type": "ready",
                           "learner_id": self.id}  # 发送ready响应信号回去
                elif var["type"] == "commit":
                    # 如果接收到leader发出的commit请求，则开始开始执行请求
                    rsp = {"type": "ack"}  # 发送ack响应信号回去

                    # 有概率发送失败
                    # if random.randrange(100) < (100 - PACKET_LOSS):
                    #     self.queue_to_learner.put(rsp)
                    #     print_str("发送回应")
                    # else:
                    #     print_str(self.name + "   >>>>>    发送回应失败")
                self.queue_send.put(rsp)
                send_msg_num += 1
                print_str("发送回应")
            except Empty:
                continue


if __name__ == '__main__':
    q_to_proposers = []  # proposer通讯的消息队列
    q_to_acceptors = []  # acceptor通讯的消息队列
    q_to_learners = []  # learner通讯的消息队列
    q_to_leader = Queue()  # leader通讯的消息队列
    start_time = time.time()
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

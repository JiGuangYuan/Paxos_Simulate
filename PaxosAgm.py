# -*- coding=utf-8 -*-
"""

该demo程序主要用来模拟分布式计算中Paxos算法的应用
系统分为四个部分：
Leader：议案的提出者，负责提出议案并等待各个Leaner表决
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

# 超时丢弃时间
OVER_TIME = 5
# 网络丢包率
PACKET_LOSS = 20
# 网络发送时延
SEND_DELAY = 0
# proposers的数量
proposers_num = 3
# acceptors的数量
acceptors_num = 5
# learners的数量
learners_num = 5


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


class Leader(threading.Thread):
    """
    Paxos Leader决议管理者
    负责所有的决议管理和编号分发
    """

    def __init__(self, t_name, queue_to_leader, queue_to_proposers, acceptor_num):
        """
        构造函数
        :param t_name: Leader名称
        :param queue_to_leader: Leader接收请求的队列
        :param queue_to_proposers: 和proposer通讯的消息队列
        :param acceptor_num: acceptor数量，用来生成acceptor编号
        """
        threading.Thread.__init__(self, name=t_name)
        self.queue_recv_list = queue_to_leader
        self.queue_send_list = queue_to_proposers
        # acceptor列表
        self.acceptor_list = list(range(0, acceptor_num))
        self.value_index = 0
        # 议案内容
        self.values = ["[议案：A]",
                       "[议案：B]",
                       "[议案：C]",
                       "[议案：D]",
                       "[议案：E]",
                       "[议案：F]",
                       "[议案：G]",
                       "[议案：H]"]
        # 议案编号
        self.value_num = 100

    def run(self):
        while True:
            # 接收请求，分配议案
            var = self.queue_recv_list.get()
            rsp = {}
            # 请求数据
            if var["type"] == "request":
                # 接收到数据"
                # 随机分配半数以上的acceptors
                acceptors_random = random.sample(self.acceptor_list, len(self.acceptor_list))
                self.value_index = random.randrange(8)
                rsp = {
                    "value": self.values[self.value_index],  # 议案内容
                    "value_num": self.value_num,  # 议案编号
                    "acceptors": acceptors_random  # 表决者列表
                }
                self.value_num += 1

            # 更新接收者列,将失败的acceptor删除，并重新随机分配一个不在失败列表里面的
            elif var["type"] == "renew":
                var_list = var["list"]
                for acceptor_fail in var["failure"]:
                    var_list.remove(acceptor_fail)
                    tmp = random.sample(self.acceptor_list, 1)
                    while tmp[0] in var["failure"]:
                        tmp = random.sample(self.acceptor_list, 1)
                    var_list.append(tmp[0])
                rsp = {
                    "list": var_list
                }

            self.queue_send_list[var["ID"]].put(rsp)


class Proposer(threading.Thread):
    """
    Paxos Proposer决议发起者
    议案的提出者，负责提出议案并等待各个接收者的表决
    """

    def __init__(self, t_name, queue_to_leader, queue_from_acceptor, queue_to_acceptors, p_num):
        """
        初始化
        :param t_name: Proposer名称
        :param queue_to_leader: Proposer和leader通信的队列
        :param queue_from_acceptor: Proposer和acceptor通讯的消息队列
        :param queue_to_acceptors: Proposer接收消息队列
        :param p_num: Proposer ID号
        """
        threading.Thread.__init__(self, name=t_name)
        self.queue_to_leader = queue_to_leader
        self.queue_recv = queue_from_acceptor
        self.queue_send_list = queue_to_acceptors
        self.num = p_num
        self.reject = 0
        self.accept = 0
        self.chosen = 0
        self.start_propose = False
        self.fail_list = []
        self.v_num = []
        self.value = ""
        self.acceptors = []
        self.var = []
        self.time_start = 0

    def run(self):
        # 从leader那里获取数据
        self.get_value_from_leader()
        # 给自己发送一个start信号
        start_sig = {"type": "start"}
        self.queue_recv.put(start_sig)
        # 循环接收从acceptor过来的消息
        while True:
            try:
                # 阻塞调用,至多阻塞1秒
                var = self.queue_recv.get(True, 1)
                # 接收到消息，准备处理
                self.process_msg(var)
            except Empty:
                # 没有接受到消息
                if self.start_propose is True and time.time() - self.time_start > OVER_TIME:
                    print_str("#######  " + self.name + "的本轮决议" + self.value + "投票结束，Accept:" + str(
                        self.accept) + " ,Reject:" + str(
                        self.reject) + " ,Chosen: " + str(self.chosen))
                    self.start_propose = False
                    if self.reject > 0:
                        print_str(
                            "-------------    " + self.name + "的决议" + self.value + "被否决，停止提议，退出    ---------------")
                    if self.chosen == len(self.acceptors):
                        print_str(
                            ">>>>>>>>>>>>>>>    " + self.name + "的决议" + self.value + "被同意，完成决议过程    <<<<<<<<<<<<<<<")
                    if (self.accept > 0 or (len(self.acceptors) > self.chosen > 0 and self.reject == 0) or
                            (self.accept == 0 and self.chosen == 0 and self.reject == 0)):
                        self.reject = 0
                        self.chosen = 0
                        self.accept = 0
                        self.send_propose()
                continue

    def get_value_from_leader(self):
        """
        从Leader那里获取数据
        :return:
        """
        req = {
            "type": "request",
            "ID": self.num
        }
        print_str(self.name + "从Leader处获取数据...")
        self.queue_to_leader.put(req)
        info = self.queue_recv.get()
        # 从Leader处获取的议案数据
        self.v_num = info["value_num"]
        self.value = info["value"]
        self.acceptors = info["acceptors"]

    def process_msg(self, var):
        """
        处理报文
        :param var: 消息报文
        :return:
        """
        # 如果是启动命令，启动程序
        if var["type"] == "start":
            self.send_propose()
        # 如果是acceptor过来的报文，解析报文
        if var["type"] == "accepting":
            # 超时丢弃
            if time.time() - self.time_start > OVER_TIME:
                print_str("消息报文超时失效，丢弃...")
                self.fail_list.append(var["acceptor"])
            else:
                if var["result"] == "reject":
                    self.reject += 1
                elif var["result"] == "accept":
                    self.accept += 1
                    # 修改决议为acceptor建议的决议
                    self.value = var["value"]
                    self.var = {
                        "type": "proposing",
                        "V_num": self.v_num,
                        "Value": var["value"],
                        "proposer": self.num
                    }
                elif var["result"] == "chosen":
                    self.chosen += 1

    def send_propose(self):
        """
        发送议案给acceptor
        :return:
        """
        self.time_start = time.time()
        self.start_propose = True
        # 模拟发送时延50ms-1000ms
        time.sleep(1 / random.randrange(1, 20))
        print_str(self.name + "  发出了一个决议，内容为:" + str(self.value))
        for acceptor in self.acceptors:
            # 提出决议，有概率发送失败
            if random.randrange(100) < (100 - PACKET_LOSS):
                self.var = {
                    "type": "proposing",
                    "V_num": self.v_num,
                    "Value": self.value,
                    "proposer": self.num,
                    "time": self.time_start
                }
                print_str(
                    self.name + "   >>>>>    " + "类型：" + self.var['type'] + " ，编号：" + str(self.var['V_num']) + " ，决议：" +
                    str(self.var['Value']) + ' ,日期：' + time.strftime("%Y-%m-%d %H:%M:%S",
                                                                     time.localtime(self.time_start)))
                self.queue_send_list[acceptor].put(self.var)
            else:
                print_str(self.name + "   >>>>>    发送提议失败")

            time.sleep(1 / random.randrange(1, 10))


class Acceptor(threading.Thread):
    """
    paxos 决议表决者acceptor
    负责接收proposer的决议并进行表决
    """

    def __init__(self, t_name, queue_from_proposer, queue_to_proposers, m_num):
        threading.Thread.__init__(self, name=t_name)
        self.queue_recv = queue_from_proposer
        self.queue_to_proposers = queue_to_proposers
        self.num = m_num
        self.values = {
            "last": 0,  # 最后一次表决的议案编号
            "value": "",  # 最后一次表决的议案的内容
            "max": 0}  # 承诺的最低表决议案编号

    def run(self):

        while True:
            try:
                var = self.queue_recv.get(False, 1)
                rsp = self.process_propose(var)
                # 有概率发送失败
                if random.randrange(100) < (100 - PACKET_LOSS):
                    self.queue_to_proposers[var["proposer"]].put(rsp)
                else:
                    print_str(self.name + "   >>>>>    发送审批失败")
            except Empty:
                continue

    def process_propose(self, value):
        """
         处理议案提出者提出的决议
        :param value: 决议的值
        :return: 响应报文
        """
        # 如果从来没接收过议案，更新自身议案
        if self.values["max"] == 0 and self.values["last"] == 0:
            self.values["max"] = value["V_num"]
            self.values["last"] = value["V_num"]
            self.values["value"] = value["Value"]
            res = {
                "type": "accepting",
                "result": "accept",
                "last": 0,
                "value": self.values["value"],
                "acceptor": self.num,
                "time": value["time"]}
        else:
            # 如果接收的议案编号大于承诺最低表决的议案编号，同意并告知之前表决结果
            if self.values["max"] < value["V_num"]:
                self.values["max"] = value["V_num"]
                res = {
                    "type": "accepting",
                    "result": "accept",
                    "last": self.values["last"],
                    "value": self.values["value"],
                    "acceptor": self.num,
                    "time": value["time"]}
            elif self.values["max"] == value["V_num"]:
                # 如果收到的议案编号等于承诺最低表决的议案编号，完全同意议案，表决结束

                self.values["last"] = value["V_num"]
                self.values["value"] = value["Value"]
                res = {
                    "type": "accepting",
                    "result": "chosen",
                    "last": self.values["last"],
                    "value": self.values["value"],
                    "acceptor": self.num,
                    "time": value["time"]
                }
            else:
                # 如果收到的议案小于承诺最低表决的议案，直接拒绝
                res = {
                    "type": "accepting",
                    "result": "reject",
                    "last": self.values["last"],
                    "value": self.values["value"],
                    "acceptor": self.num,
                    "time": value["time"]
                }
        return res


if __name__ == '__main__':

    q_to_proposers = []  # proposer通讯的消息队列
    q_to_acceptors = []  # acceptor通讯的消息队列
    q_to_learners = []  # leaner通讯的消息队列

    q_leader_to_proposers = []
    q_to_leader = Queue()  # 接收请求的队列

    for i in range(0, proposers_num):
        q_to_proposers.append(Queue())
        q_leader_to_proposers.append(Queue())

    for i in range(0, acceptors_num):
        q_to_acceptors.append(Queue())

    for i in range(0, learners_num):
        q_to_learners.append(Queue())

    ld = Leader("Leader", q_to_leader, q_to_proposers, acceptors_num)
    ld.setDaemon(True)
    ld.start()

    for i in range(0, proposers_num):
        proposer_th = Proposer("proposer'" + str(i) + "'", q_to_leader, q_to_proposers[i], q_to_acceptors, i)
        proposer_th.setDaemon(True)
        proposer_th.start()

    for i in range(0, acceptors_num):
        acceptor_th = Acceptor("Acceptor'" + str(i) + "'", q_to_acceptors[i], q_to_proposers, i)
        acceptor_th.setDaemon(True)
        acceptor_th.start()

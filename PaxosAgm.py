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


class Proposer(threading.Thread):
    """
    Paxos Proposer决议发起者
    议案的提出者，负责提出议案并等待各个接收者的表决
    """

    def __init__(self, t_name, queue_from_acceptor, queue_to_acceptors, p_num):
        """
        构造函数
        :param t_name: Proposer名称
        :param queue_from_acceptor: Proposer接收消息队列
        :param queue_to_acceptors: Proposer和acceptor通讯的消息队列
        :param p_num: Proposer ID号
        """
        threading.Thread.__init__(self, name=t_name)
        self.queue_recv = queue_from_acceptor
        self.queue_send = queue_to_acceptors
        self.num = p_num  # proposer的id
        self.reject = 0  # 拒绝的acceptor计数
        self.accept = 0  # 接受的acceptor计数
        self.chosen = 0  # 选择的acceptor计数
        self.start_propose = False  # proposer开始标志
        self.fail_list = []  # 超时失效的消息队列
        self.v_num = 100 + self.num  # 申请的编号
        self.value = t_name + "申请成为leader"  # 申请的内容
        self.acceptors = range(acceptors_num)  # acceptors的列表
        self.var = {}  # 发送的消息
        self.time_start = 0

    def run(self):
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
                # 投票结束了
                if self.start_propose is True and time.time() - self.time_start > OVER_TIME:
                    print_str("#######  " + self.name + "的本次申请" + self.value + "投票结束，Accept:" + str(
                        self.accept) + " ,Reject:" + str(
                        self.reject) + " ,Chosen: " + str(self.chosen))
                    self.start_propose = False
                    # 判断投票结果
                    if self.reject > 0:
                        # 如果有一个拒绝则被否决
                        print_str(
                            "-------------    " + self.name + "的申请" + self.value + "被否决，重新申请    ---------------")
                        self.reject = 0
                        self.chosen = 0
                        self.accept = 0
                        self.send_propose()
                        continue
                        # if self.chosen > len(self.acceptors) / 2:
                    elif self.chosen == len(self.acceptors):
                        # 如果超过半数chosen则被同意
                        print_str(
                            ">>>>>>>>>>>>>>>    " + self.name + "的申请" + self.value + "被同意，完成表决过程    <<<<<<<<<<<<<<<")
                        print_str("-------------------------- " + self.name + "：成为了Leader-----------------------")
                        # Proposer竞争为leader成功，启动leader进程
                        ld = Leader("Leader", q_to_leader, q_to_learners)
                        ld.start()
                        # (改进) proposer直接告之其它Proposer停止申请，设置proposer全局变量
                        for acceptor in self.acceptors:
                            self.var = {"status": "stop", "proposer": self.num}
                            print_str("结束选举")
                            self.queue_send[acceptor].put(self.var)
                    elif(self.accept > 0 or
                            (len(self.acceptors) > self.chosen > 0 and self.reject == 0) or
                            (self.accept == 0 and self.chosen == 0 and self.reject == 0)):
                        # 网络原因获取回应失败，重新开始申请
                        self.reject = 0
                        self.chosen = 0
                        self.accept = 0
                        self.send_propose()
                continue

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
                if var["result"] == "reject":
                    # 拒绝的申请编号+1
                    self.reject += 1
                    self.v_num = var["max_val"] + 1
                elif var["result"] == "accept":
                    self.accept += 1
                    """
                    # 修改决议为acceptor建议的决议
                    self.value = var["value"]
                    self.var = {
                        "status": "start",
                        "type": "proposing",
                        "V_num": self.v_num,
                        "Value": var["value"],
                        "proposer": self.num
                    }

                    """
                elif var["result"] == "chosen":
                    self.chosen += 1
            else:
                # 超时接收,则丢弃
                print_str("消息报文超时失效，丢弃...")
                self.fail_list.append(var["acceptor"])

    def send_propose(self):
        """
        发送一次申请给所有acceptor
        :return:
        """
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
                    "proposer": self.num,
                    "time": self.time_start
                }
                print_str(
                    self.name + "   >>>>>    " + "类型：" + self.var["type"] + " ，编号：" + str(self.v_num) + " ，决议：" +
                    self.var["Value"] + " ,日期：" + time.strftime("%Y-%m-%d %H:%M:%S",
                                                                time.localtime(self.time_start)))
                self.queue_send[acceptor].put(self.var)
            else:
                print_str(self.name + "   >>>>>    发送申请失败")

            time.sleep(1 / random.randrange(1, 10))


class Acceptor(threading.Thread):
    """
    paxos 申请表决者acceptor
    负责接收proposer的申请进行表决
    """

    def __init__(self, t_name, queue_from_proposer, queue_to_proposers, m_num):
        """
        构造函数
        :param t_name: Acceptor的名称
        :param queue_from_proposer: 从proposer收到的消息队列
        :param queue_to_proposers: 向proposer发送的消息队列
        :param m_num: acceptor的ID
        """
        threading.Thread.__init__(self, name=t_name)
        self.queue_recv = queue_from_proposer
        self.queue_send = queue_to_proposers
        self.num = m_num
        self.values = {
            "last": 0,  # 最后一次表决的申请编号
            "value": "",  # 最后一次表决的申请的内容
            "max": 0}  # 承诺的最低表决申请编号

    def run(self):

        while True:
            try:
                var = self.queue_recv.get(False, 1)
                rsp = self.process_propose(var)
                '''
                # 有概率发送失败
                if random.randrange(100) < (100 - PACKET_LOSS):
                    self.queue_send[var["proposer"]].put(rsp)
                else:
                    print_str(self.name + "   >>>>>    发送审批失败")
                '''
                self.queue_send[var["proposer"]].put(rsp)
            except Empty:
                continue

    def process_propose(self, value):
        """
         处理申请提出者提出的申请
        :param value: 决议的值
        :return: 响应报文
        """
        if value["status"] == "stop":
            res = {"type": "stop"}
        # 如果从来没接收过申请，更新自身申请
        elif self.values["max"] == 0 and self.values["last"] == 0:
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
            # 如果接收的申请编号大于承诺最低表决的申请编号，同意并告知之前表决结果
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
                # 如果收到的申请编号等于承诺最低表决的申请编号，完全同意申请，表决结束
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
                # 如果收到的申请小于承诺最低表决的申请，直接拒绝
                res = {
                    "max_val": self.values["max"],
                    "type": "accepting",
                    "result": "reject",
                    "last": self.values["last"],
                    "value": self.values["value"],
                    "acceptor": self.num,
                    "time": value["time"]
                }
        return res


class Leader(threading.Thread):
    """
    Paxos Leader 决议提出者
    决议的提出者，负责提出决议并等待各个Leaner表决
    """

    def __init__(self, t_name, queue_from_learner, queue_to_learner):
        """
        构造函数
        :param t_name: Leader名称
        :param queue_from_learner: Leader从learner接收请求的队列
        :param queue_to_learner: 和learner通讯的消息队列
        """

        threading.Thread.__init__(self, name=t_name)
        self.queue_recv = queue_from_learner
        self.queue_send = queue_to_learner
        self.recv_ack_num = 0

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

        rsp = {"type": "prepare"}
        for n in range(0, learners_num):
            # 发送prepare信号给learner
            self.queue_send[n].put(rsp)
            print_str("发送 prepare 信号")
        index = random.randrange(8)
        while True:

            # 接收learner回复的状态信息
            try:
                var = self.queue_recv.get()
                rsp = {
                    "type": "commit",
                    "value": self.values[index],  # 议案内容
                    "value_num": self.value_num,  # 议案编号
                }
                # 获取ready信息
                if var["type"] == "ready":
                    # 发送commit信号给learner
                    self.queue_send[var["id"]].put(rsp)
                    print_str("发送 commit 信号")
                # （改进） 如果没有接收到learner的全部回应，则立即重新发送prepare请求
                # 获取ack成功
                elif var["type"] == "ack":
                    self.recv_ack_num += 1
                    if self.recv_ack_num == learners_num:
                        self.recv_ack_num = 0
                        # 如果接收全部learner的ack回应，则表示成功
                        print_str(">>>>>>>>这次分布式一致性决议,完成<<<<<<<<<")
                        # 如果没有接收到全部learner的ack回应，则重复此过程
            except Empty:
                continue


class Learner(threading.Thread):
    """
    Learner负责响应Leader 的决议，并执行决议
    """

    def __init__(self, t_name, queue_from_leader, queue_to_leader, l_num):
        """
        构造函数
        :param t_name:  Learner的名称
        :param queue_from_leader:   从leader传来的消息队列
        :param queue_to_leader:   向leader发送的消息队列
        :param l_num:    l ID
        :return:
        """
        threading.Thread.__init__(self, name=t_name)
        self.queue_recv = queue_from_leader
        self.queue_send = queue_to_leader
        self.num = l_num

    def run(self):

        while True:
            try:
                rsp = {}
                var = self.queue_recv.get(False, 1)
                print_str("----------------------------------------------------------------------------------")
                print_str(var)
                if var["type"] == "prepare":
                    # 接收到prepare信息，则发送ready信号回去
                    rsp = {"type": "ready",
                           "id": self.num}  # 发送ready响应信号回去
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
                print_str("发送回应")
            except Empty:
                continue


if __name__ == '__main__':

    q_to_proposers = []  # proposer通讯的消息队列
    q_to_acceptors = []  # acceptor通讯的消息队列
    q_to_learners = []  # learner通讯的消息队列
    q_to_leader = Queue()  # leader通讯的消息队列

    for i in range(0, proposers_num):
        q_to_proposers.append(Queue())
        proposer_th = Proposer("proposer'" + str(i) + "'", q_to_proposers[i], q_to_acceptors, i)
        proposer_th.setDaemon(True)
        proposer_th.start()

    for i in range(0, acceptors_num):
        q_to_acceptors.append(Queue())
        acceptor_th = Acceptor("Acceptor'" + str(i) + "'", q_to_acceptors[i], q_to_proposers, i)
        acceptor_th.setDaemon(True)
        acceptor_th.start()

    # learner进程的开启
    for i in range(0, learners_num):
        q_to_learners.append(Queue())
        learner_th = Learner("Learner'" + str(i) + "'", q_to_learners[i], q_to_leader, i)
        learner_th.setDaemon(True)
        learner_th.start()

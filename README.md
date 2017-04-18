# Paxos_Simulate
模拟Paxos运行环境，以及改进算法的运行

# Paxos_Simulate
模拟Paxos运行环境，以及改进算法的运行

# 算法改进:
## 竞选leader阶段：
   对于网络不稳定的情况下，直接取消一次Paxos选举leader的过程，使用Bully算法来简化Leader选举的过程
## 申请接受阶段：
针对角色proposer的行为优化
  - 在一个proposer被告之他的proposal编号不是当前最大的时候，就重新提出一个编号更大的提案，这样会导致在网络不稳定的情况下，极大概率引发冲突。此时proposer可以随机等待一段时间，再提出一个编号更大的proposal进行竞争。
  - 增加一个消息队列，proposer增加对其它proposer发来请求的监听，选为leader的proposer直接发送消息让所有Proposer停止申请，
        
## 议案接受阶段：
### 针对角色learner的行为优化
  - 在一个learner收到更大编号的proposal时，可以向它已经ready过的proposer提早发出nack消息，这样可以较低通信负载。
  - 在learner收到leader的commit请求的时候，判断状态需要返回reject的时候，直接无视leader的请求，leader超时自己自动编号+1重新提出议案。
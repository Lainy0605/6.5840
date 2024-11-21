**GO 切片底层复用**

rf.log = rf.log[:startLogIndex]
http://jjzhong.com/2023/08/20/MIT%206.5840%20%E5%88%86%E5%B8%83%E5%BC%8F%E7%B3%BB%E7%BB%9F%EF%BC%88Spring%202023%EF%BC%89%E4%B8%A8Lab%202%EF%BC%9ARaft%E7%9A%84%E6%9E%81%E8%87%B4%E4%BC%98%E5%8C%96%EF%BC%8CPart%202D%E6%B5%8B%E8%AF%95%E6%97%B6%E9%97%B4%E7%BC%A9%E7%9F%AD%E5%88%B0115s%EF%BC%81/

**ApplyCh死锁**

ch缓存，https://juejin.cn/post/7347973138787860490#heading-5

**TestFigure8Unreliable2C**

网络抖动，导致RPC乱序到达，需判断如果日志没有发生冲突，短日志不应该覆盖长日志，https://www.calvinneo.com/2019/03/12/raft-algorithm/
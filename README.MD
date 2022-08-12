## 只写
selfTest --op="insert" -c 12 --insertNum=1000000 --beginId=1000 --dataSize=1024 --noUseCache
## 读写
selfTest --op="get-set" -c 12 --dataSize=1024 --onlineNum=10000 --stepId=200 --stayTm="15s" --beginId=1000 --sendLimit=80 --limitStepCnt=10

## 对bigcache改进考虑：
1.读也纳入活跃（目前只有写)(读存在穿透到badger)  
2.value改为指针(或模板,提供值长度)，避免重复序列号开销(记录长度，用于空间不足以淘汰最旧为判断)

## 说明:
1. 如果是一次性插入大量数据的话 使用badger原生的接口会更快，这个库适合相同key多次更新的情况下，可以减少写入频次，稳定内存  
2.读后写入不会阻塞，初次读取可能会阻塞(需要从badger拉取数据)  
3.事务读写使用缓存能提高约4倍性能.(具体参考doc)

## 其他具体数值测试参考doc目录下的文档.

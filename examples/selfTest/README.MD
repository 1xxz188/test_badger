只写
selfTest --op="insert" -c 1 --insertNum=50000 --beginId=1000
读写
selfTest --op="get-set" -c 12 --dataSize=128 --onlineNum=10 --stepId=12 --stayTm="10s" --beginId=1000 --sendLimit=1 --limitStepCnt=300

//TODO 对bigcache改进考虑：
1.读也纳入活跃（目前只有写)(读存在穿透到badger)
2.value改为指针(或模板,提供值长度)，避免重复序列号开销(记录长度，用于空间不足以淘汰最旧为判断)

//TODO
*缓存层使用包装模式
*初始背景数据使用原生badger并发插入

//目前缺陷：
1.当写入速度小于更新速度时（瞬时大量更新或插入数据时)，未过期的移除可能会阻塞前端读写。(因为有watchKey读锁,写锁)

如果是一次性插入大量数据的话 使用badger原生的接口会更快，这个库适合相同key多次更新的情况下，可以减少写入频次，稳定内存。
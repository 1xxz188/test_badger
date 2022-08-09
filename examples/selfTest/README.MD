只写
selfTest --op="insert" -c 1 --insertNum=50000 --beginId=1000
读写
selfTest --op="get-set" -c 12 --dataSize=128 --onlineNum=10 --stepId=12 --stayTm="10s" --beginId=1000 --sendLimit=1 --limitStepCnt=300

//TODO 对bigcache改进考虑：
1.读也纳入活跃（目前只有写)(读存在穿透到badger)
2.value改为指针(或模板,提供值长度)，避免重复序列号开销(记录长度，用于空间不足以淘汰最旧为判断)
只写
selfTest --op="insert" -c 1 --insertNum=50000 --beginId=1000
读写
selfTest --op="get-set" -c 12 --dataSize=128 --onlineNum=10 --stepId=12 --stayTm="10s" --beginId=1000 --sendLimit=1 --limitStepCnt=300
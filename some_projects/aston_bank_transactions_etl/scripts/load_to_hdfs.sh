#local
scp bank_transactions.csv security_transactions.csv securities.csv clients.csv s.ashkinadzi@172.17.0.23:
#remote
hdfs dfs -mkdir /user/s.ashkinadzi
hdfs dfs -put -f bank_transactions.csv security_transactions.csv securities.csv clients.csv/user/s.ashkinadzi
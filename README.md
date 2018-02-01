# Migration2.0to2.1

### DCD C extension update

1. 새로운 계정으로 IRIS 2.1 패키징
2. ~/IRIS/lib/Ext , user_store 복사(마스터 포함 모든 노드)
3. IRIS 재시작하여 정상 동작 확인

### DLD  hash_mod update

0. m6.config hash_mod = 5 추가
1. master > 모든 노드 disable
2. slaves > PL RAM
3. slaves > ~/IRIS/data/slave_disk 전체 백업
4. master > data/master 백업
5. all nodes > library update (2.0 --> 2.1)
6. all nodes > IRIS-Shutdown
7. master > data/master 복구
8. DLD_Migration script를 ~/IRIS/data/master에 놓고 실행

### SYS_TABLE_INFO <-- SYS_TABLE_REALNAME

1. master > 상기 작업 완료 후 migration 스크립트 실행

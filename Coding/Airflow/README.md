# ETL(Extract, Transform and Load) 작성하기

파이썬으로 간단한 ETL을 작성해 보고, Airflow가 어떻게 도움이 되는지 알아봅니다.   
앞서 작성한 간단한 ETL을 Airflow로 변환해 봅니다.

## 4 주차에는 이런 고민을 해소합니다

* ETL이 구체적으로 무엇인가요?
* Airflow와 같은 프레임워크를 사용하면 뭐가 편해지나요?
* Airflow 대신, FiveTran이나 StitchData와 같은 SaaS를 사용하는 것에 차이가 있나요?

## 과제

### Colab Python 예제에 있는 두 가지 문제를 수정하기 

1. 헤더의 값이 들어가는 이슈 제거 
2. 현재 잡을 수행할 때마다 테이블에 레코드들이 들어가는데 이를 idempotent하게 수정하기

코드 : [DB_Insert_Transaction_With.py](DB_Insert_Transaction_With.py) 

### Colab Python 예제를 Airflow의 DAG 포맷으로 바꿔보기  

그런데 말씀드렸던 것처럼 extract, transform, load를 각기 task로 만들지말고 하나의 task안에서 이 세가지를 다 호출하는 걸로 해보세요.   

그 이유는 Airflow의 경우 태스크들간의 데이터 이동이 복잡해서 그렇습니다 (Xcom이란 기능 사용 필요). 

코드 : [Air_DB_Insert_Transaction_With.py](Air_DB_Insert_Transaction_With.py)
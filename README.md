airflow

v0.8.0/yoda/dev -> cleansing operator 추가

문서 찍먹하기 : https://www.notion.so/Airflow-docker-airflow-a5ec30b6e391443f9443a2f6fae88e46?pvs=4


airflow 띄우기

1. docker image 만들기
```
sudo docker build -t airflow:base .
```

2. 생성된 이미지로 compose up
```
sudo docker compose up
```

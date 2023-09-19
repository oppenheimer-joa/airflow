# airflow

## v0.9.0/rel 

### 릴리즈 History

- v0.7.0/test -> v0.1.0 ~ v0.6.0 병합 및 테스트

- v0.8.0/yoda/dev -> cleansing operator 추가

- v0.8.1/na/dev -> blob s3 operator 추가

- v0.8.1/test 병합 BR 목록
  - v0.8.0/yoda/dev
  - v0.8.1/na/dev

- v0.9.0/rel -> spark job submit 대비 Template 스크립트 작성

### 전체 Dag 목록

- Kobis(BoxOffice) 수집 Dag
- Kopis(공연정보) 수집 Dag
- IMDB(영화제) 수집 Dag
- TMDB(영화 credit, detail, image, people, similar) 수집 Dag


### Airflow 간편 구성
문서 : https://www.notion.so/Airflow-docker-airflow-a5ec30b6e391443f9443a2f6fae88e46?pvs=4
airflow 띄우기

1. docker image 만들기
```
sudo docker build -t airflow:base .
```

2. 생성된 이미지로 compose up
```
sudo docker compose up
```

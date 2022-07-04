from pandas import json_normalize # 판다스 설치 안했으면, pip install pandas 로 설치해주기

__all__ = ["preprocessing"]

def preprocessing(ti):
    # ti(task instance) - dag 내의 task의 정보를 얻어 낼 수 있는 객체

    # xcom(cross communication) - Operator와 Operator 사이에 데이터를 전달 할 수 있게끔 하는 도구
    search_result = ti.xcom_pull(task_ids=["crawl_naver"])

    # xcom을 이용해 가지고 온 결과가 없는 경우
    if not len(search_result):
        raise ValueError("검색 결과 없음")
    
    items = search_result[0]["items"]
    processed_items = json_normalize([
        {"title": item["title"],
         "address": item["address"],
         "category": item["category"],
         "description": item["description"],
         "link": item["link"]} for item in items
    ])
    
    processed_items.to_csv("/home/kurran/airflow/dags/data/naver_processed_result.csv", index=None, header=False)


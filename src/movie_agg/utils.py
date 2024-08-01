import pandas as pd

def merge(load_dt=20240724):
    import numpy as np

    read_df = pd.read_parquet("/home/root2/tmp/test_parquet/")
    cols = [
            'movieCd',      # 영화의 대표코드를 출력합니다.
            'movieNm',      # 영화명(국문)을 출력합니다.
         #   'openDt',       # 영화의 개봉일을 출력합니다.
         #   'audiCnt',      # 해당일의 관객수를 출력합니다.
            'load_dt',      # 입수일자
            'multiMovieYn', # 다양성영화 유무
            'repNationCd',  # 한국외국영화 유무
            ]
    
    df = read_df[cols]
    
   #df_where=df[df["movieCd"]=="20247781"].copy()

    df_where =df[np.logical_and(df["movieCd"]=="20247781",df["load_dt"]==load_dt)].copy()

    print(df_where)
    print(df_where.dtypes)

    df_where["load_dt"] = df_where["load_dt"].astype("object")
    df_where["multiMovieYn"] = df_where["multiMovieYn"].astype("object")
    df_where["repNationCd"] = df_where["repNationCd"].astype("object")

    print(df_where.dtypes)
    
    
    df_where['multiMovieYn'] = df_where['multiMovieYn'].fillna('unknown')
    df_where['repNationCd'] = df_where['repNationCd'].fillna('unknown')

    u_mul=df_where[df_where["multiMovieYn"]!="unknown"]
    u_nat=df_where[df_where["repNationCd"]!="unknown"]

    m_df=pd.merge(u_mul, u_nat, on="movieCd", suffixes=["_m","_n"])

    print("merged DF")
    print(m_df)

    
    return m_df


merge()

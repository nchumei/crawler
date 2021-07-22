import pandas as pd
import re
import pyodbc
import sys
from sqlalchemy import create_engine


def deal_diama(text):
    text = re.sub(r"－", "-", text)
    text = re.sub(r"（", "(", text)
    text = re.sub(r"）", ")", text)
    text = re.sub(r",", "", text)
    text = re.sub(r"\s{0,}", "", text)
    return text


def creator():
    return pyodbc.connect(r'Driver={SQL Server};Server=DESKTOP-OKF0JOA;Database=financial_report;Trusted_Connection=yes;')


Engine = create_engine('mssql://', creator=creator)
cnx = Engine.connect()

daima = pd.read_csv("D:/User/Desktop/corpus/mops/會科代碼.csv", encoding="CP950")
stock_list = pd.read_csv("D:/User/Desktop/corpus/mops/stock_normal_web.csv", encoding="cp950")

df_stocklist = pd.DataFrame({"代號Code": ['ID', 'Name', 'Type', 'Section', 'Time', 'Remark']})
df_full = pd.DataFrame({"代號Code": list(daima["代號"][0:773])}).sort_values("代號Code")
df_full = pd.merge(df_stocklist, df_full, on='代號Code', how="outer")
df_append = df_full.T
df_append.columns = list(df_append.loc["代號Code"])

for sk in range(stock_list.shape[0]):
    address = "D:/User/Desktop/corpus/report/2021Q1/balance_sheet/" + str(stock_list["代號"][sk]) + "_2021Q1.csv"
    df = pd.read_csv(address, encoding="cp950", index_col=0)
    df = df.dropna().reset_index(drop=True)
    df = df[[df.columns[0], df.columns[2]]]
    for colu in [1]:
        for i in range(df.shape[0]):
            try:
                df[df.columns[colu]][i] = int("-" + re.findall("\((.*?)\)", deal_diama(str(df[df.columns[colu]][i])))[0])
            except IndexError:
                df[df.columns[colu]][i] = int(deal_diama(str(df[df.columns[colu]][i])))

    df = pd.merge(df_full, df, on='代號Code', how="outer")
    df = df.fillna(0)

    if "2021年3月31日2021/3/31" in df.columns:
        df = df.T
        df.columns = list(df.loc["代號Code"])
    else:
        print("年份錯誤", stock_list["代號"][sk])
        sys.exit()
    for colu in [1]:
        # 資產 = 流動資產  + 非流動資產
        if df["1XXX"][colu] == df["11XX"][colu] + df["15XX"][colu]:
            pass
        else:
            print(stock_list["代號"][sk], "資產error")
            sys.exit()

        # 負債 = 流動負債 + 非流動負債
        if df["2XXX"][colu] == df["21XX"][colu] + df["25XX"][colu]:
            pass
        else:
            print(stock_list["代號"][sk],"負債error")
            sys.exit()

        # 權益
        if df["31XX"][colu] == 0:
            print("Before ", df["31XX"][colu])
            df["31XX"][colu] = df["3100"][colu] + df["3199"][colu] + df["3200"][colu] +\
            df["3300"][colu] + df["3400"][colu] + df["3500"][colu]
            print(stock_list["代號"][sk], "[31XX] change into  ", df["31XX"][colu])

        # 權益總和 = 歸屬於母公司業主之權益合計 + 非控制權益
        if df["3XXX"][colu] == df["31XX"][colu] + df["36XX"][colu] + df["35XX"][colu]:
            pass
        else:
            print(stock_list["代號"][sk], "權益error")
            sys.exit()

        # 負債及權益總計
        if df["3X2X"][colu] == df["2XXX"][colu] + df["3XXX"][colu]:
            pass
        else:
            print(stock_list["代號"][sk], "負債及權益總計error")
            sys.exit()

        # 財報平衡
        if df["1XXX"][colu] == df["2XXX"][colu] + df["3XXX"][colu]:
            pass
        else:
            print("財報未平衡 : ",stock_list["代號"][sk])
            sys.exit()

        df["ID"][colu] = str(stock_list['代號'][sk])
        df["Name"][colu] = stock_list['name'][sk]
        df["Type"][colu] = stock_list['type'][sk]
        df["Section"][colu] = stock_list['section'][sk]

    df["Time"][1] = '2021-03-01'  # [03-01,06-01,09-01,12-01]
    df["Remark"][1] = '2021年3月31日'  # [3月31日,6月30日,9月30日,12月31日]
    # [1月1日至3月31日,4月1日至6月30日,7月1日至9月30日,10月1日至12月31日]

    df_append = df_append.append(df.drop(["代號Code"]))

df_append = df_append.drop(['代號Code'])

for i in range(6, df_append.shape[1]):
    df_append = df_append.rename(columns={df_append.columns[i]: "a_" + df_append.columns[i]})

df_append.to_csv("D:/User/Desktop/corpus/mops/2021Q1_balancesheet.csv", encoding="cp950", index=False)
df_append.to_sql("balance_sheet", cnx, index=False, if_exists="append", chunksize=10000)


# ---------income statement-----------

df_stocklist = pd.DataFrame({"代號Code": ['ID', 'Name', 'Type', 'Section', 'Time', 'Remark']})
df_full = pd.DataFrame({"代號Code": list(daima["代號"][773:1234])})
df_full = pd.merge(df_stocklist, df_full, on='代號Code', how="outer").drop_duplicates()
df_append = df_full.T
df_append.columns = list(df_append.loc["代號Code"])

for sk in range(stock_list.shape[0]):
    address = "D:/User/Desktop/corpus/report/2021Q1/income_statement/" + str(stock_list["代號"][sk]) + "_2021Q1.csv"
    df = pd.read_csv(address, encoding="cp950", index_col=0)
    df = df.dropna().reset_index(drop=True)
    df = df[[df.columns[0], df.columns[2]]]
    print(stock_list["代號"][sk])

    for colu in [1]:
        for i in range(len(df)):
            if type(df.loc[df.index[i], df.columns[0]]) ==str:
                pass
            else:
                df.loc[df.index[i], df.columns[0]] = str(int(df.loc[df.index[i], df.columns[0]]))

            if "(" in str(df.loc[df.index[i], df.columns[colu]]):
                df.loc[df.index[i], df.columns[colu]] ="-" + re.findall("\((.*?)\)", deal_diama(
                    df.loc[df.index[i], df.columns[colu]]))[0]
            else:
                df.loc[df.index[i], df.columns[colu]] = deal_diama(df.loc[df.index[i], df.columns[colu]])
    df = pd.merge(df_full, df, on='代號Code', how="left")

    if "2021年1月1日至3月31日2021/1/1To3/31" in df.columns:
        df = df.T
        df.columns = list(df.loc["代號Code"])
    else:
        print("年份錯誤", stock_list["代號"][sk])
        sys.exit()

    df.loc[df.index[1], "ID"] = str(stock_list['代號'][sk])
    df.loc[df.index[1], "Name"] = stock_list['name'][sk]
    df.loc[df.index[1], "Type"] = stock_list['type'][sk]
    df.loc[df.index[1], "Section"] = stock_list['section'][sk]
    df.loc[df.index[1], "Time"] = '2021-03-01'
    df.loc[df.index[1], "Remark"] = '2021年1月1日至3月31日'

    df_append = df_append.append(df.drop(["代號Code"]))

df_append = df_append.drop(['代號Code'])
df_append = df_append.fillna(0)

for i in range(6,df_append.shape[1]):
    df_append = df_append.rename(columns={df_append.columns[i] : "a_"+ df_append.columns[i]})

df_append.to_csv("D:/User/Desktop/corpus/mops/2021Q1_income_statement.csv", encoding="cp950",index = False)
df_append.to_sql("income_statement", cnx, index=False, if_exists="append", chunksize=10000)

# when q4

df_append = pd.read_csv("D:/User/Desktop/corpus/mops/2020Q4_income_statement.csv", encoding="cp950")
conn = pyodbc.connect("DRIVER={SQL Server};SERVER=DESKTOP-OKF0JOA;DATABASE=mops")
cursor = conn.cursor()

for i in range(0,df_append.shape[0]):
    word = df_append["ID"][i]
    sql_cmd_q1 = "select * from income_statement where ID ="+"'"+str(word)+"'" + "and Time between '2020-03-01' and '2020-12-01'"
    dfq1 = pd.read_sql(sql_cmd_q1, conn)
    dfq1 = dfq1.sort_values(by="Time")
    df_colsum = dfq1.sum()
    df_append.iloc[i,6:] = df_append.iloc[i,6:] - df_colsum[6:]
    print(i)

df_append.to_csv("D:/User/Desktop/corpus/mops/2020Q4_income_statement_deal.csv", encoding="cp950", index=False)
df_append.to_sql("income_statement", cnx, index=False, if_exists="append", chunksize=10000)


# ----------cash_flow------------

df_stocklist = pd.DataFrame({"代號Code": ['ID', 'Name', 'Type', 'Section', 'Time', 'Remark']})
df_full = pd.DataFrame({"代號Code": list(daima["代號"][1234:])})
df_full = pd.merge(df_stocklist, df_full, on='代號Code', how="outer").drop_duplicates()
df_append = df_full.T
df_append.columns = list(df_append.loc["代號Code"])


for sk in range(stock_list.shape[0]):
    address = "D:/User/Desktop/corpus/report/2021Q1/cash_flow/" + str(stock_list["代號"][sk])+"_2021Q1.csv"
    df = pd.read_csv(address, encoding="cp950", index_col=0)
    df = df.dropna().reset_index(drop=True)
    df = df[[df.columns[0], df.columns[2]]]
    print(stock_list["代號"][sk])
    for colu in [1]:
        for i in range(len(df)):
            try:
                df[df.columns[colu]][i] = int(
                    "-" + re.findall("\((.*?)\)", deal_diama(str(df[df.columns[colu]][i])))[0])
            except IndexError:
                df[df.columns[colu]][i] = int(deal_diama(str(df[df.columns[colu]][i])))

    df = pd.merge(df_full, df, on='代號Code', how="outer")
    df = df.fillna(0)

    if "2021年1月1日至3月31日2021/1/1To3/31" in df.columns:
        df = df.T
        df.columns = list(df.loc["代號Code"])
    else:
        print("年份錯誤", stock_list["代號"][sk])
        sys.exit()

    df.loc[df.index[1], "ID"] = str(stock_list['代號'][sk])
    df.loc[df.index[1], "Name"] = stock_list['name'][sk]
    df.loc[df.index[1], "Type"] = stock_list['type'][sk]
    df.loc[df.index[1], "Section"] = stock_list['section'][sk]
    df.loc[df.index[1], "Time"] = '2021-03-01'
    df.loc[df.index[1], "Remark"] = '2021年1月1日至3月31日'

    df_append = df_append.append(df.drop(["代號Code"]))


df_append = df_append.drop(['代號Code'])

for i in range(6, df_append.shape[1]):
    df_append = df_append.rename(columns={df_append.columns[i] : "a_"+ df_append.columns[i]})

df_append.to_csv("D:/User/Desktop/corpus/mops/2021Q1_cash_flow.csv", encoding="cp950", index=False)
df_append.to_sql("cash_flow", cnx, index=False, if_exists="append", chunksize=10000)


for i in range(0,df_append.shape[0]):
    word = df_append["ID"][i]
    sql_cmd_q1 = "select * from cash_flow where ID ="+"'"+str(word)+"'" + "and Time between '2020-03-01' and '2020-12-01'"
    dfq1 = pd.read_sql(sql_cmd_q1, conn)
    dfq1 = dfq1.sort_values(by="Time")
    df_colsum = dfq1.sum()
    df_append.iloc[i,6:] = df_append.iloc[i,6:] - df_colsum[6:]
    print(i)

df_append.to_csv("D:/User/Desktop/corpus/mops/2020Q4_cash_flow_deal.csv", encoding="cp950", index=False)
df_append.to_sql("cash_flow", cnx, index=False, if_exists="append", chunksize=10000)



import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import json
import os
import plotly.express as px


import mysql.connector
import requests


def clean_states_list(df,States_col='States'):
    df[States_col] = df[States_col].replace({
        "andaman-&-nicobar-islands": "Andaman and Nicobar",
        "dadra-&-nagar-haveli-&-daman-&-diu": "Dadra and Nagar Haveli and Daman Diu"
    })

    df[States_col] = df[States_col].str.replace("-"," ")
    df[States_col] = df[States_col].str.title()


    return df

#DataFrame creation
#aggre_insurance
path7= "C:/Users/Jayavelu/VSCode/pulse/data/aggregated/insurance/country/india/state/"

agg_insur_list= os.listdir(path7)

columns7= {"States":[], "Years":[], "Quarter":[], "Insurance_type":[], "Insurance_count":[],"Insurance_amount":[] }

for States in agg_insur_list:
    cur_States = path7+States+"/"
    agg_year_list = os.listdir(cur_States)

    for year in agg_year_list:
        cur_years = cur_States+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            A = json.load(data)
            try:
             for i in A["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns7["Insurance_type"].append(name)
                columns7["Insurance_count"].append(count)
                columns7["Insurance_amount"].append(amount)
                columns7["States"].append(States)
                columns7["Years"].append(year)
                columns7["Quarter"].append(int(file.strip(".json")))
            except:
             pass




aggre_insurance = pd.DataFrame(columns7)

Aggr_Ins = clean_states_list(aggre_insurance)

#aggre_transaction
path1 = "C:/Users/Jayavelu/VSCode/pulse/data/aggregated/transaction/country/india/state/"

agg_tran_list = os.listdir(path1)

columns1 ={"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[],"Transaction_amount":[] }

for States in agg_tran_list:
    cur_States =path1+States+"/"
    agg_year_list = os.listdir(cur_States)

    for year in agg_year_list:
        cur_years = cur_States+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            B = json.load(data)

            for i in B["data"]["transactionData"]:
                name = i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns1["Transaction_type"].append(name)
                columns1["Transaction_count"].append(count)
                columns1["Transaction_amount"].append(amount)
                columns1["States"].append(States)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))

aggre_transaction = pd.DataFrame(columns1)

Aggr_Trans=clean_states_list(aggre_transaction)

#aggre_user
path2 = "C:/Users/Jayavelu/VSCode/pulse/data/aggregated/user/country/india/state/"

agg_user_list = os.listdir(path2)

columns2 = {"States":[], "Years":[], "Quarter":[], "Brands":[],"Transaction_count":[], "Percentage":[]}

for States in agg_user_list:
    cur_States = path2+States+"/"
    agg_year_list = os.listdir(cur_States)

    for year in agg_year_list:
        cur_years = cur_States+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            C = json.load(data)

            try:

                for i in C["data"]["usersByDevice"]:
                    brand = i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    columns2["Brands"].append(brand)
                    columns2["Transaction_count"].append(count)
                    columns2["Percentage"].append(percentage)
                    columns2["States"].append(States)
                    columns2["Years"].append(year)
                    columns2["Quarter"].append(int(file.strip(".json")))

            except:
                pass

aggre_user = pd.DataFrame(columns2)

Aggr_User=clean_states_list(aggre_user)

#map_insurance
path8= "C:/Users/Jayavelu/VSCode/pulse/data/map/insurance/hover/country/india/state/"

map_insur_list= os.listdir(path8)

columns8= {"States":[], "Years":[], "Quarter":[], "Districts":[], "Transaction_count":[],"Transaction_amount":[] }

for States in map_insur_list:
    cur_States =path8+States+"/"
    agg_year_list = os.listdir(cur_States)

    for year in agg_year_list:
        cur_years = cur_States+year+"/"
        agg_file_list = os.listdir(cur_years)

        for file in agg_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            D = json.load(data)

            for i in D["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns8["Districts"].append(name)
                columns8["Transaction_count"].append(count)
                columns8["Transaction_amount"].append(amount)
                columns8["States"].append(States)
                columns8["Years"].append(year)
                columns8["Quarter"].append(int(file.strip(".json")))


map_insurance = pd.DataFrame(columns8)

Map_Ins=clean_states_list(map_insurance)

#map_transaction
path3 = "C:/Users/Jayavelu/VSCode/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list = os.listdir(path3)

columns3 = {"States":[], "Years":[], "Quarter":[],"District":[], "Transaction_count":[],"Transaction_amount":[]}

for States in map_tran_list:
    cur_States = path3+States+"/"
    map_year_list = os.listdir(cur_States)

    for year in map_year_list:
        cur_years = cur_States+year+"/"
        map_file_list = os.listdir(cur_years)

        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            E = json.load(data)

            for i in E['data']["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns3["District"].append(name)
                columns3["Transaction_count"].append(count)
                columns3["Transaction_amount"].append(amount)
                columns3["States"].append(States)
                columns3["Years"].append(year)
                columns3["Quarter"].append(int(file.strip(".json")))

map_transaction = pd.DataFrame(columns3)

Map_Trans=clean_states_list(map_transaction)

#map_user
path4 = "C:/Users/Jayavelu/VSCode/pulse/data/map/user/hover/country/india/state/"
map_user_list = os.listdir(path4)

columns4 = {"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUser":[], "AppOpens":[]}

for States in map_user_list:
    cur_States = path4+States+"/"
    map_year_list = os.listdir(cur_States)

    for year in map_year_list:
        cur_years = cur_States+year+"/"
        map_file_list = os.listdir(cur_years)

        for file in map_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            F = json.load(data)

            for i in F["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appopens = i[1]["appOpens"]
                columns4["Districts"].append(district)
                columns4["RegisteredUser"].append(registereduser)
                columns4["AppOpens"].append(appopens)
                columns4["States"].append(States)
                columns4["Years"].append(year)
                columns4["Quarter"].append(int(file.strip(".json")))

map_user = pd.DataFrame(columns4)

Map_User=clean_states_list(map_user)

#top_insurance

path9 = "C:/Users/Jayavelu/VSCode/pulse/data/top/insurance/country/india/state/"

top_insur_list = os.listdir(path9)

columns9 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for States in top_insur_list:
    cur_States = path9+States+"/"
    top_year_list = os.listdir(cur_States)

    for year in top_year_list:
        cur_years = cur_States+year+"/"
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            G = json.load(data)

            for i in G["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns9["Pincodes"].append(entityName)
                columns9["Transaction_count"].append(count)
                columns9["Transaction_amount"].append(amount)
                columns9["States"].append(States)
                columns9["Years"].append(year)
                columns9["Quarter"].append(int(file.strip(".json")))

top_insur = pd.DataFrame(columns9)

Top_Ins=clean_states_list(top_insur)


#top_transaction
path5 = "C:/Users/Jayavelu/VSCode/pulse/data/top/transaction/country/india/state/"
top_tran_list = os.listdir(path5)

columns5 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for States in top_tran_list:
    cur_States = path5+States+"/"
    top_year_list = os.listdir(cur_States)

    for year in top_year_list:
        cur_years = cur_States+year+"/"
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            H = json.load(data)

            for i in H["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns5["Pincodes"].append(entityName)
                columns5["Transaction_count"].append(count)
                columns5["Transaction_amount"].append(amount)
                columns5["States"].append(States)
                columns5["Years"].append(year)
                columns5["Quarter"].append(int(file.strip(".json")))

top_transaction = pd.DataFrame(columns5)

Top_Trans=clean_states_list(top_transaction)


#top_user
path6 = "C:/Users/Jayavelu/VSCode/pulse/data/top/user/country/india/state/"
top_user_list = os.listdir(path6)

columns6 = {"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUser":[]}

for States in top_user_list:
    cur_States = path6+States+"/"
    top_year_list = os.listdir(cur_States)

    for year in top_year_list:
        cur_years = cur_States+year+"/"
        top_file_list = os.listdir(cur_years)

        for file in top_file_list:
            cur_files = cur_years+file
            data = open(cur_files,"r")
            I = json.load(data)

            for i in I["data"]["pincodes"]:
                name = i["name"]
                registeredusers = i["registeredUsers"]
                columns6["Pincodes"].append(name)
                columns6["RegisteredUser"].append(registeredusers)
                columns6["States"].append(States)
                columns6["Years"].append(year)

                columns6["Quarter"].append(int(file.strip(".json")))

top_user = pd.DataFrame(columns6)

Top_User=clean_states_list(top_user)



from sqlalchemy import create_engine
import pymysql  
engine = create_engine('mysql+pymysql://root@localhost:3306/phonepe')
                        
def run_query(query, params=None):
    with engine.connect() as con:
        return pd.read_sql(query, con=engine, params=params)

Aggr_Trans.to_sql('aggregated_transaction', con=engine, if_exists='replace', index=False)
Aggr_User.to_sql('aggregated_user', con=engine, if_exists='replace', index=False)
Aggr_Ins.to_sql('aggregated_insurance', con=engine, if_exists='replace', index=False)
Map_Ins.to_sql('map_insurance', con=engine, if_exists='replace', index=False)
Map_Trans.to_sql('map_transaction', con=engine, if_exists='replace', index=False)
Map_User.to_sql('map_user', con=engine, if_exists='replace', index=False)
Top_Ins.to_sql('top_insurance', con=engine, if_exists='replace', index=False)
Top_Trans.to_sql('top_transaction', con=engine, if_exists='replace', index=False)
Top_User.to_sql('top_user', con=engine, if_exists='replace', index=False)


#converting dataframe into a list
Years = run_query("SELECT DISTINCT Years FROM aggregated_transaction ORDER BY Years")["Years"].tolist()
Quarter = run_query("SELECT DISTINCT Quarter FROM aggregated_transaction ORDER BY Quarter")["Quarter"].tolist()
states_list=run_query("SELECT DISTINCT States FROM aggregated_transaction ORDER BY States")["States"].tolist()
st.set_page_config(page_title="PhonePe Pulse Insights", layout="wide")

st.title("PhonePe Pulse Insights")


with st.sidebar:
    selected = option_menu("Menu", ["Home","Explore Data"],
                icons=["house","bar-chart-line"],
                menu_icon= "menu-button-wide",
                default_index=0,
                styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#9a78eb"},
                        "nav-link-selected": {"background-color": "#5d78a3"}})

if selected == "Home":
    st.sidebar.header("Data Visualization based on Transaction")
    
    Years = st.selectbox('**Choose a Year**', Years,index=len(Years)-1)
    Quarter = st.selectbox('**Choose a Quarter**',Quarter, index=len(Quarter)-1)

    df = run_query(f"""
        SELECT States, SUM(Transaction_amount) AS TotalAmount
        FROM aggregated_transaction
        WHERE Years=%s AND Quarter=%s
        GROUP BY States
    """, params=(Years, Quarter))
    
    df1=run_query(f"""
        SELECT States, SUM(Insurance_amount) AS TotalAmount
        FROM aggregated_insurance
        WHERE Years=%s AND Quarter=%s
        GROUP BY States
    """, params=(Years, Quarter))
    


    state_mapping = {
    "andaman-&-nicobar-islands": "Andaman & Nicobar",
    "andhra-pradesh": "Andhra Pradesh",
    "arunachal-pradesh": "Arunachal Pradesh",
    "assam": "Assam",
    "bihar": "Bihar",
    "chandigarh": "Chandigarh",
    "chhattisgarh": "Chhattisgarh",
    "dadra-&-nagar-haveli-&-daman-&-diu": "Dadra and Nagar Haveli and Daman and Diu",
    "delhi": "NCT of Delhi",
    "goa": "Goa",
    "gujarat": "Gujarat",
    "haryana": "Haryana",
    "himachal-pradesh": "Himachal Pradesh",
    "jammu-&-kashmir": "Jammu & Kashmir",
    "jharkhand": "Jharkhand",
    "karnataka": "Karnataka",
    "kerala": "Kerala",
    "ladakh": "Ladakh",
    "lakshadweep": "Lakshadweep",
    "madhya-pradesh": "Madhya Pradesh",
    "maharashtra": "Maharashtra",
    "manipur": "Manipur",
    "meghalaya": "Meghalaya",
    "mizoram": "Mizoram",
    "nagaland": "Nagaland",
    "odisha": "Odisha",
    "puducherry": "Puducherry",
    "punjab": "Punjab",
    "rajasthan": "Rajasthan",
    "sikkim": "Sikkim",
    "tamil-nadu": "Tamil Nadu",
    "telangana": "Telangana",
    "tripura": "Tripura",
    "uttar-pradesh": "Uttar Pradesh",
    "uttarakhand": "Uttarakhand",
    "west-bengal": "West Bengal"
    }
    st.markdown("<h1 style ='color: red;'> Transaction based on Amount</h1>", unsafe_allow_html=True)
    df=df.replace({"State": state_mapping})
    fig = px.choropleth(
    df,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='States',
    color='TotalAmount',
    color_continuous_scale='Reds'
    )

    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig)
    st.markdown("<h2 style ='color: red;'> Insurance Analysis by Amount</h2>", unsafe_allow_html=True)
    df1=df1.replace({"State": state_mapping})
    fig1 = px.choropleth(
    df1,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='States',
    color='TotalAmount',
    color_continuous_scale='Blues'
    )

    fig1.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig1)

elif selected == "Explore Data":
    
    st.sidebar.header("Explore Data")
    st.subheader("Business Use Cases")
    Type = st.selectbox("**Business Cases**", ("Transaction_Analysis", "User Engagement across States", "Insurance Penetration and Growth Potential", "Device Usage Analysis", "Top 10 Transaction across States"))
    Years = st.selectbox('**Choose a Year**', Years,index=len(Years)-1)
    Quarter = st.selectbox('**Choose a Quarter**',Quarter, index=len(Quarter)-1)
    selected_States = st.selectbox("**Choose a State**", states_list)

    if Type == "Device Usage Analysis":
       
        st.markdown("<h1 style ='color: red;'>Device Usage Analysis</h1>", unsafe_allow_html=True)
        try:
            df = run_query("""
    SELECT Brands, SUM(Transaction_count) as total_count 
    FROM aggregated_user
    WHERE Years = %s AND Quarter = %s AND States = %s
    GROUP BY Brands
        """, params=(Years, Quarter, selected_States))

        
            fig_1=px.treemap(df, path=["Brands"], values="total_count", title="Device Usage Treemap")
            st.plotly_chart(fig_1, use_container_width=True)
        except:
            st.error("No data available for device usage analysis.")

    elif Type == "Transaction_Analysis":
        st.markdown("<h1 style ='color: red;'>Transaction Analysis</h1>", unsafe_allow_html=True)
        df=run_query("""SELECT Transaction_type,SUM(Transaction_amount) AS Amt 
                     FROM aggregated_transaction where Years=%s AND Quarter=%s  GROUP BY Transaction_type
                     """,params=(Years,Quarter))
        fig1=px.pie(df,names="Transaction_type", values="Amt", title="Transaction Analysis by Amount", hole=0.5)
        st.plotly_chart(fig1, use_container_width=True)
        df1=run_query("""SELECT Transaction_type,SUM(Transaction_count) AS totalCount 
                     FROM aggregated_transaction where Years=%s AND Quarter=%s  GROUP BY Transaction_type
                     """,params=(Years,Quarter))
        fig1=px.pie(df1,names="Transaction_type", values="totalCount", title="Transaction Analysis by Count",hole=0.5)
        st.plotly_chart(fig1, use_container_width=True)
        

    elif Type == "Insurance Penetration and Growth Potential":
        st.markdown("<h1 style ='color: red;'>Insurance Penetration and Growth Potential</h1>", unsafe_allow_html=True)
        df = run_query("""
       SELECT States,Years,SUM(Transaction_amount) as Total, SUM(Transaction_count) AS TotalCount FROM `map_insurance` WHERE States=%s GROUP BY Years,Quarter ORDER BY Years

    """,params=(selected_States,))
        fig1 = px.area(df, x="Years", y="Total", hover_name="States",  title="Insurance Amount Over Years",width=400,
    height=400)
       
       
        st.plotly_chart(fig1, use_container_width=True)
        df1 = run_query("""
       SELECT States,Years,SUM(Transaction_count) as Total FROM `map_insurance` WHERE States=%s GROUP BY Years,Quarter ORDER BY Years

    """,params=(selected_States,))
        fig2=px.area(df1, x="Years", y="Total", hover_name="States", title="Insurance Count Over Years",width=400,
    height=400)
    
        st.plotly_chart(fig2, use_container_width=True)

    elif Type == "User Engagement across States":
        st.markdown("<h2 style ='color: red;'>User Engagement across States</h2>", unsafe_allow_html=True)
        df1=run_query("""SELECT States,SUM(AppOpens) AS TotAppOpens  FROM map_user WHERE Years = %s AND Quarter = %s 
                         GROUP BY States ORDER BY TotAppOpens DESC limit 10
                      """, params=(Years, Quarter))

        fig1 = px.bar(df1, x="States", y="TotAppOpens", title="Top 10 States by App Opens")
        st.plotly_chart(fig1, use_container_width=True)
    
        df = run_query("""
           SELECT States,  SUM(RegisteredUser) AS TotRegisteredUsers FROM map_user WHERE Years = %s AND Quarter = %s GROUP BY States
           ORDER BY TotRegisteredUsers DESC limit 10
       """, params=(Years, Quarter))
        fig=px.pie(df, names="States", values="TotRegisteredUsers", title="Top 10 States by User Registrations", hole=0.5)
        st.plotly_chart(fig, use_container_width=True)
        

    elif Type == "Top 10 Transaction across States":
        st.markdown("<h2 style ='color: red;'>Transaction across States</h2>", unsafe_allow_html=True)
        st.subheader("Transaction across States")

        df1=run_query("""SELECT States,SUM(Transaction_count) as amt,SUM(Transaction_count) as Cnt FROM `map_insurance` WHERE Years= %s AND Quarter=%s GROUP BY States""", params=(Years, Quarter))

        fig1 = px.bar(df1, x="States", y="amt", hover_name="States", labels={"x":"States", "y":"Transaction Amount"}, title="Transaction across States")
        st.plotly_chart(fig1, use_container_width=True)
        df2=run_query("""SELECT States,SUM(Transaction_count) as amt,SUM(Transaction_count) as Cnt FROM `map_insurance` WHERE Years= %s AND Quarter=%s GROUP BY States ORDER BY Cnt DESC limit 10""", params=(Years, Quarter))
        fig2 = px.bar(df2, x="States", y="Cnt",  labels={"x":"States", "y":"Transaction Count"}, title="Top 10 States by Transaction Count")
        st.plotly_chart(fig2, use_container_width=True)
        
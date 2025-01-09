# Input dataframe for CLV processing 

import lifetimes
import lifetimes.utils
import pandas as pd
import numpy as np
import datetime as dt 
import matplotlib.pyplot as plt
import seaborn as sns 

from lifetimes import BetaGeoFitter
from lifetimes import GammaGammaFitter
from sklearn.preprocessing import MinMaxScaler

from lifetimes.plotting import plot_frequency_recency_matrix
from lifetimes.plotting import plot_probability_alive_matrix
from lifetimes.plotting import plot_period_transactions

from pathlib import Path
import os

def find_boundaries(df,variable, q1=0.05, q2=0.95):
    lower_boundary  = df[variable].quantile(q1)
    upperr_boundary  = df[variable].quantile(q2)
    return lower_boundary,upperr_boundary

def cap_outliers(df,variable):
    lb, ub = find_boundaries(df,variable)
    df[variable] = np.where(df[variable] > ub,ub, np.where(df[variable] <lb,lb,df[variable]))
    

def lv( df):
    df = df[df['Quantity']>0]
    df = df[df['UnitPrice']>0]
    df.dropna(inplace=True)

    cap_outliers(df,"UnitPrice")
    cap_outliers(df,"Quantity")

    df["TotalPurchase"] = df['Quantity'] * df['UnitPrice']

    clv = lifetimes.utils.summary_data_from_transaction_data(df,"CustomerID","InvoiceDate","TotalPurchase",observation_period_end ='2024-11-25'  )
    clv =  clv[clv['frequency']>1]

    bgf = BetaGeoFitter(penalizer_coef=0.001)
    bgf.fit(clv['frequency'],clv['recency'],clv['T'])
    # print(bgf.summary)
    # plt.figure(figsize=(8,6))
    # plot_probability_alive_matrix(bgf)
    # plt.show()

    t= 180 
    clv["ExpectedPurchaseFor_6_Months"] = bgf.conditional_expected_number_of_purchases_up_to_time(t,clv["frequency"],clv["recency"],clv["T"])
    print(clv.sort_values(by="ExpectedPurchaseFor_6_Months", ascending=False).head())
    print(clv[["frequency","monetary_value"]].corr())

    ggf = GammaGammaFitter(penalizer_coef=0.01)
    ggf.fit(clv["frequency"], clv["monetary_value"] ) 

    clv["6_Months_CLV"] = ggf.customer_lifetime_value(bgf,clv["frequency"],clv["recency"],clv["T"],clv["monetary_value"],time=6,freq='D',discount_rate=0.01)
    print(clv.sort_values(by="6_Months_CLV", ascending=False).head())
    clv["Segment"] = pd.qcut(clv["6_Months_CLV"],q=4,labels=["Hibernating","Need Attendion","Loyal Customers","Champions"])
    print(clv.head())
    print(clv.groupby("Segment").mean())
    return clv.sort_values(by="6_Months_CLV",ascending= False)
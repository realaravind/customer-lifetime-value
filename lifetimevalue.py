# Input dataframe for CLV processing 

import lifetimes
import lifetimes.utils
import pandas as pd
import numpy as np
import datetime as dt 
import matplotlib.pyplot as plt
import seaborn as sns 

from lifetimes import BetaGeoFitter,ParetoNBDFitter
from lifetimes import GammaGammaFitter
from sklearn.preprocessing import MinMaxScaler

from lifetimes.plotting import plot_frequency_recency_matrix
from lifetimes.plotting import plot_probability_alive_matrix
from lifetimes.plotting import plot_period_transactions

from pathlib import Path
import os
import threading

def infer_correlation(r):
    """
    Judge the strength of a correlation based on its value.
    
    Parameters:
        r (float): The correlation coefficient.
        
    Returns:
        str: Description of the correlation strength.
    """
    if not -1 <= r <= 1:
        return "Invalid correlation value. It should be between -1 and 1."
    
    if abs(r) < 0.3:
        return "Weak"
    elif abs(r) < 0.7:
        return "Moderate"
    else:
        return "Strong"



def find_boundaries(df,variable, q1=0.05, q2=0.95):
    lower_boundary  = df[variable].quantile(q1)
    upperr_boundary  = df[variable].quantile(q2)
    return lower_boundary,upperr_boundary

def cap_outliers(df,variable):
    lb, ub = find_boundaries(df,variable)
    df[variable] = np.where(df[variable] > ub,ub, np.where(df[variable] <lb,lb,df[variable]))

def paretonbd_lv(df, p_pred_days,p_noof_months,p_observation_period_end):
    print('Pareto NBD Start')
    cap_outliers(df,"UnitPrice")
    cap_outliers(df,"Quantity")
    # df['Total Price'] = df['UnitPrice'] * df['Quantity']

    clv = lifetimes.utils.summary_data_from_transaction_data(df,"CustomerID","InvoiceDate","Total Price",observation_period_end =p_observation_period_end  )
    #clv = lifetimes.utils.summary_data_from_transaction_data(df,"CustomerID","InvoiceDate","Total Price")

    clv =  clv[clv['frequency']>1]

    # Fit the Pareto/NBD model
    pareto_nbd = ParetoNBDFitter()
    pareto_nbd.fit(clv['frequency'], clv['recency'], clv['T'])
    
    t=  int(p_pred_days)
    clv["Pareto Predicted No Of Orders"] = pareto_nbd.conditional_expected_number_of_purchases_up_to_time(t,clv["frequency"],clv["recency"],clv["T"])
                                                                                         
    df_corr = clv[['frequency','monetary_value']].corr()
    corr = round(df_corr.iloc[1, 0],2)
    print(f"Pareto Corr: {corr}")

    clv['Pareto Alive Probability'] = pareto_nbd.conditional_probability_alive(clv['frequency'], clv['recency'], clv['T'])
    ggf = GammaGammaFitter()
    ggf.fit(clv['frequency'], clv['monetary_value'])

    clv['Pareto Predicted Clv'] = ggf.customer_lifetime_value(pareto_nbd, clv['frequency'], clv['recency'], clv['T'], clv['monetary_value'],time=int(p_noof_months),  # Predict for 12 months
        discount_rate=0.01  # Discount rate
    )
    
    # def plot_pareto_probability_alive_matrix():
    #     plt.figure(figsize=(12, 8))
    #     plot_probability_alive_matrix(pareto_nbd)
    #     plt.savefig('static/pareto_probability_alive_matrix.png')
    #     plt.close()
    
    
    # plot_thread = threading.Thread(target=plot_pareto_probability_alive_matrix)
    # plot_thread.start()
    clv["Pareto Segment"] = pd.qcut(clv["Pareto Predicted Clv"],q=4,labels=["Hibernating","Need Attention","Loyal Customers","Champions"])
    clv["Pareto Predicted No Of Orders"] = clv["Pareto Predicted No Of Orders"].fillna(0)
    clv["Pareto Predicted No Of Orders"] = round(clv["Pareto Predicted No Of Orders"],0).astype(int)
    clv["Pareto Predicted Clv"] = clv["Pareto Predicted Clv"].fillna(0)
    clv["Pareto Predicted Clv"] = round(clv["Pareto Predicted Clv"],0).astype(int)
    clv['Pareto Alive Probability'] = round(clv['Pareto Alive Probability'],1)
    
    return clv,df_corr

def lv( df, p_pred_days,p_noof_months,p_observation_period_end):
   

    cap_outliers(df,"UnitPrice")
    cap_outliers(df,"Quantity")
    # df['Total Price'] = df['UnitPrice'] * df['Quantity']
    
   
   
    clv = lifetimes.utils.summary_data_from_transaction_data(df,"CustomerID","InvoiceDate","Total Price",observation_period_end =p_observation_period_end  )
    #clv = lifetimes.utils.summary_data_from_transaction_data(df,"CustomerID","InvoiceDate","Total Price")

    clv =  clv[clv['frequency']>1]

    bgf = BetaGeoFitter(penalizer_coef=0.001)
    bgf.fit(clv['frequency'],clv['recency'],clv['T'])
    # print(bgf.summary)
    # plt.figure(figsize=(8,6))
    # plot_probability_alive_matrix(bgf)
    # plt.show()
    
    t=  int(p_pred_days)
    
    clv["PredictedNoOfOrders"] = bgf.conditional_expected_number_of_purchases_up_to_time(t,clv["frequency"],clv["recency"],clv["T"])
   
    
    df_corr = clv[['frequency','monetary_value']].corr()

    corr = round(df_corr.iloc[1, 0],2)

    ggf = GammaGammaFitter(penalizer_coef=0.01)
    ggf.fit(clv["frequency"], clv["monetary_value"] ) 
    
    clv["PredictedCLV"] = ggf.customer_lifetime_value(bgf,clv["frequency"],clv["recency"],clv["T"],clv["monetary_value"],time=int(p_noof_months),freq='D',discount_rate=0.01)
    

    clv["Segment"] = pd.qcut(clv["PredictedCLV"],q=4,labels=["Hibernating","Need Attention","Loyal Customers","Champions"])
   

    clv["Segment"] = clv["Segment"].cat.add_categories(["Unpredicted"]).fillna("Unpredicted")
    clv["PredictedNoOfOrders"] = clv["PredictedNoOfOrders"].fillna(0)
    clv["PredictedCLV"] = clv["PredictedCLV"].fillna(0)
    
   
    return clv.sort_values(by="PredictedCLV",ascending= False), df_corr
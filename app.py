from flask import Flask, request, render_template, jsonify, send_file, Response
import pandas as pd
import os
from lifetimevalue import lv,infer_correlation,paretonbd_lv  # Replace with actual import if using a specific library
import json
from collections import OrderedDict
from dateutil.relativedelta import relativedelta
import numpy as np

app = Flask(__name__)

# Route for the main page
@app.route('/')
def index():
    return render_template('index.html')  # Your main upload and filter page

@app.route('/get-filters', methods=['POST'])
def get_filters():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    # Read the uploaded Excel file
    df = pd.read_excel(file)

    # Extract unique values for filters (example logic)
    filter1_values = df['Column1'].unique()  # Replace with actual column names
    filter2_values = df['Column2'].unique()  # Replace with actual column names
    filter3_values = df['Column3'].unique()  # Replace with actual column names

    return jsonify({
        "filter1": filter1_values[0] if len(filter1_values) > 0 else '',
        "filter2": filter2_values[0] if len(filter2_values) > 0 else '',
        "filter3": filter3_values[0] if len(filter3_values) > 0 else ''
    })

@app.route('/process', methods=['POST'])
def process():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
  
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    # Read the uploaded Excel file
    df = pd.read_excel(file)

    df = df[df['Quantity'] > 0 ] # exclude the orders with 0 value
    df = df[df['UnitPrice'] > 0] # exclude the Unit Price with 0 value
    
    df.dropna(inplace=True)     

       

    # Get filter values from the request
    cut_off = int(request.form.get('cutOff'))
    filter1 = request.form.get('filter1')
    filter2 = request.form.get('filter2')
    churnRule = request.form.get('churnRule')
    threshold = request.form.get('threshold')
   
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
    max_date = df['InvoiceDate'].max() - relativedelta(months=cut_off)

    actual_max_date_to_agg = max_date + relativedelta(months=cut_off)

    no_of_months = int(cut_off)
    noofdays = no_of_months*30
   
    df['Total Price'] = df['UnitPrice'] * df['Quantity']

    print(f"Max Date : {max_date}")

    df_actual_after_cut_off = df[df['InvoiceDate']>max_date]

    actual_order_summary  = df_actual_after_cut_off[df_actual_after_cut_off["InvoiceDate"] <=actual_max_date_to_agg].groupby('CustomerID').agg(
    DistinctOrders=('Id', 'nunique'),  # Count distinct SalesOrderId
    TotalPriceSum=('Total Price', 'sum')).reset_index()
    
    actual_order_total = str(int(round(actual_order_summary["TotalPriceSum"].sum(),0)))
    actual_no_of_orders =  str(actual_order_summary["DistinctOrders"].sum())

    print(f"Actual No of Orders For the Period: {actual_no_of_orders},  Actual Order Total: {actual_order_total}")
    #print(actual_order_summary)

    q1=0.05
    q2=0.95

    lower_boundary  = df['UnitPrice'].quantile(q1)
    upper_boundary  = df['UnitPrice'].quantile(q2)

    print(f"Lower Boundary: {lower_boundary}, Upper Boundary: {upper_boundary}")
    
    output_df, df_corr = lv(df, noofdays,  no_of_months,max_date)
    print(output_df)
    pareto_output_df, pareto_df_corr = paretonbd_lv(df, noofdays,  no_of_months,max_date)
    
    pareto_output_df.reset_index()

    corr = round(df_corr.iloc[1, 0],2)
    pareto_corr = round(pareto_df_corr.iloc[1, 0],2)
    corr_cat = infer_correlation(corr)
    pareto_corr_cat = infer_correlation(pareto_corr)
    
    PredictedCLV = str(int(round(output_df["PredictedCLV"].sum(),0)))
    ParetoPredictedCLV = str(int(round(pareto_output_df["Pareto Predicted Clv"].sum(),0)))
    PredictedNoOfOrders = str(int(round(output_df["PredictedNoOfOrders"].sum(),0)))
    ParetoPredictedNoOfOrders = str(int(round(pareto_output_df["Pareto Predicted No Of Orders"].sum(),0)))

    aggregated_df = df.groupby('CustomerID', as_index=False).first()
    

    output_df = pd.merge(output_df, aggregated_df[['CustomerID', 'CustomerName']], on='CustomerID', how='left')

    output_df['frequency'] = output_df['frequency'].astype(int)
    output_df['recency'] = output_df['recency'].astype(int)
    output_df['T'] = output_df['T'].astype(int)
    output_df['PredictedNoOfOrders'] = output_df['PredictedNoOfOrders'].astype(int)
    output_df['PredictedCLV'] = output_df['PredictedCLV'].astype(int)
    output_df['monetary_value'] = output_df['monetary_value'].round().astype(int)

    pareto_output_df['frequency'] = pareto_output_df['frequency'].astype(int)
    pareto_output_df['recency'] = pareto_output_df['recency'].astype(int)
    pareto_output_df['T'] = pareto_output_df['T'].astype(int)
    pareto_output_df['Pareto Predicted No Of Orders'] = pareto_output_df['Pareto Predicted No Of Orders'].astype(int)
    pareto_output_df['Pareto Predicted Clv'] = pareto_output_df['Pareto Predicted Clv'].astype(int)
    pareto_output_df['monetary_value'] = pareto_output_df['monetary_value'].round().astype(int)
   
    print(pareto_output_df.columns)

    xchurn_label = {
    "Hibernating": 1,        # High churn probability
    "Need Attention": 1,     # Medium churn probability
    "Loyal Customers": 0,    # Low churn probability
    "Champions": 0          # Very low churn probability
    }
    print(f"churnRule: {churnRule} , Threshold:{threshold}")
    output_df['Predicted Churn Ind'] = 0 
   
    match churnRule:
        case "Segment":
            output_df["Predicted Churn Ind"] = output_df["Segment"].map(xchurn_label)
            pareto_output_df["Pareto Predicted Churn Ind"] = pareto_output_df["Pareto Segment"].map(xchurn_label)
        case "Recency":
            output_df['Predicted Churn Ind'] = ((output_df['T'].astype(int) - output_df['recency'].astype(int)) > int(threshold)).astype(int)
            pareto_output_df['Pareto Predicted Churn Ind'] = ((pareto_output_df['T'].astype(int) - pareto_output_df['recency'].astype(int)) > int(threshold)).astype(int)
        case "FM":
            output_df['Predicted Churn Ind'] = ((output_df['T'].astype(int) - output_df['recency'].astype(int)) > int(threshold)).astype(int)
            pareto_output_df['Pareto Predicted Churn Ind'] = ((pareto_output_df['T'].astype(int) - pareto_output_df['recency'].astype(int)) > int(threshold)).astype(int)

    output_df["PredictedCLV"] = output_df["PredictedCLV"].fillna(0)
    pareto_output_df["Pareto Predicted Clv"] = pareto_output_df["Pareto Predicted Clv"].fillna(0)
    
    output_df['Predicted Churn Ind'].fillna(0)
    pareto_output_df["Pareto Predicted Churn Ind"].fillna(0)

    output_df.to_excel("output.xlsx")
    output_df.to_excel("pareto_output.xlsx")
    new_column_order = ["No","CustomerID","CustomerName","frequency","recency","T","monetary_value","PredictedNoOfOrders","PredictedCLV","Segment","Predicted Churn Ind"]
    
    # Adding a row number column
    output_df['No'] = range(1, len(output_df) + 1)
    
    output_df = pd.DataFrame(output_df, columns=new_column_order)

    output_df = output_df.merge(
    actual_order_summary,  # The DataFrame to join with
    on='CustomerID',       # The column to join on
    how='left'             # Perform a left outer join
    )
    output_df = output_df.fillna({'DistinctOrders': 0, 'TotalPriceSum': 0})


    output_df['Actual Churn Ind'] = np.where((output_df['DistinctOrders'] > 0), 0, 1)

    output_df.rename(columns={'CustomerName': 'Name'}, inplace=True)
    output_df.rename(columns={'T': 'Age'}, inplace=True)
    output_df.rename(columns={'CustomerID': 'Id'}, inplace=True)
    output_df.rename(columns={'frequency': 'Frequency'}, inplace=True)
    output_df.rename(columns={'recency': 'Recency'}, inplace=True)
    output_df.rename(columns={'monetary_value': 'Monetary Value'}, inplace=True)
    output_df.rename(columns={'PredictedNoOfOrders': 'Predicted No Of Orders'}, inplace=True)
    output_df.rename(columns={'PredictedCLV': 'Predicted CLV'}, inplace=True)

    output_df.rename(columns={'DistinctOrders': 'Actual No Of Orders'}, inplace=True)
    output_df.rename(columns={'TotalPriceSum': 'Actual Order Total'}, inplace=True)

    actual_churn_count =  output_df["Actual Churn Ind"].sum()
    predicted_churn_count =  output_df["Predicted Churn Ind"].sum()
    pareto_predicted_churn_count =  pareto_output_df["Pareto Predicted Churn Ind"].sum()  
    
    print(f"Predicted Churn Count: {predicted_churn_count} , Actual Churn Count:{actual_churn_count}")
    print(f"Pareto Predicted Churn Count: {pareto_predicted_churn_count} , Actual Churn Count:{actual_churn_count}")

    if (predicted_churn_count > 0 and pareto_predicted_churn_count>0):
        ChurnRatio = round( actual_churn_count/predicted_churn_count,1)
        ParetoChurnRatio = round( actual_churn_count/pareto_predicted_churn_count,1)
    else:
        ChurnRatio = 0 
        ParetoChurnRatio =0 
    print(f"BG Churn Ratio: {ChurnRatio} , Pareto Churn Ratio:{ParetoChurnRatio}")

    pareto_output_df.reset_index(inplace = True)
    pareto_output_df.rename(columns={'CustomerID':'Id'}, inplace=True)
    
    output_df = output_df.merge(
        pareto_output_df[['Id', 'Pareto Predicted No Of Orders', 'Pareto Alive Probability', 'Pareto Predicted Clv', 'Pareto Segment','Pareto Predicted Churn Ind']],
        on='Id',
        how='left'
    )
    
    output_df.to_excel('clv_output.xlsx')
        
    results = output_df.to_dict(orient='records')  # Convert to a list of dictionaries for output
    
    results = output_df.to_dict(orient='records')

    response_data = {
    "metrics": {
        "Correlation": str(corr) + '(' + corr_cat + ')',
        "PredictedCLV": PredictedCLV,
        "ActualOrderTotal":actual_order_total,
        "PredictedNoOfOrders": PredictedNoOfOrders,
        "ActualNoOfOrders": actual_no_of_orders,
        "OrderValueRatio": round(int(actual_order_total) /int( PredictedCLV ),1)*100,
        "OrderVolumeRatio": round(int(actual_no_of_orders) /int(PredictedNoOfOrders),1)*100,
        "ChurnRatio" : ChurnRatio,

        "ParetoCorrelation": str(pareto_corr) + '(' + pareto_corr_cat + ')',
        "ParetoPredictedCLV": ParetoPredictedCLV,
        "ParetoPredictedNoOfOrders": ParetoPredictedNoOfOrders,
        "ParetoOrderValueRatio": round(int(actual_order_total) /int( ParetoPredictedCLV ),1)*100,
        "ParetoOrderVolumeRatio": round(int(actual_no_of_orders) /int(ParetoPredictedNoOfOrders),1)*100,
        "ParetoChurnRatio" : ParetoChurnRatio
    },
    "results": results
    }


    response = Response(json.dumps(response_data), mimetype="application/json")

    return response


# Route to display processed data
@app.route('/display/<filename>')
def display_data(filename):
    df = pd.read_excel(filename)  # Load the processed data

    # Replace these with your actual metric calculations if needed
    metric1 = 100 # Example metric
    metric2 = 200  # Example metric
    metric3 = 300   # Example metric

    return render_template('display.html', 
                           tables=[df.to_html(classes='data', index=False)],
                           metric1=metric1,
                           metric2=metric2,
                           metric3=metric3,
                           filter1=request.args.get('filter1'),
                           filter2=request.args.get('filter2'),
                           filter3=request.args.get('filter3'))

# Route for downloading the processed file
@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    return send_file(filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
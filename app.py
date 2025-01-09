from flask import Flask, request, render_template, send_file
import pandas as pd
import os
from lifetimevalue import lv


app = Flask(__name__)

# Route for file upload
@app.route('/')
def upload_file():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def process_file():
    if 'file' not in request.files:
        return "No file part", 400
    file = request.files['file']
    if file.filename == '':
        return "No selected file", 400

    # Process the Excel file
    df = pd.read_excel(file)
    # Perform any processing on the DataFrame
    # For example, you could filter, sort, etc.

    output_df = lv(df)
    aggregated_df = df.groupby('CustomerID', as_index=False).first()

    output_df = pd.merge(output_df, aggregated_df[['CustomerID', 'CustomerName']], on='CustomerID', how='left')

    # Convert frequency and recency to integers
    output_df['frequency'] = output_df['frequency'].astype(int)
    output_df['recency'] = output_df['recency'].astype(int)
    output_df['T'] = output_df['T'].astype(int)
    output_df['ExpectedPurchaseFor_6_Months'] = output_df['ExpectedPurchaseFor_6_Months'].astype(int)
    output_df['6_Months_CLV'] = output_df['6_Months_CLV'].astype(int)




    # Round monetary_value and convert to integer
    output_df['monetary_value'] = output_df['monetary_value'].round().astype(int)

    new_column_order = ["No","CustomerID","CustomerName","frequency","recency","T","monetary_value","ExpectedPurchaseFor_6_Months","6_Months_CLV","Segment"]
    
    # Adding a row number column
    output_df['No'] = range(1, len(output_df) + 1)
    output_df = output_df[new_column_order]

    output_df.rename(columns={'CustomerName': 'Name'}, inplace=True)
    output_df.rename(columns={'T': 'Age'}, inplace=True)
    output_df.rename(columns={'CustomerID': 'Id'}, inplace=True)
    output_df.rename(columns={'frequency': 'Frequency'}, inplace=True)
    output_df.rename(columns={'recency': 'Recency'}, inplace=True)
    output_df.rename(columns={'monetary_value': 'Monetary Value'}, inplace=True)
    output_df.rename(columns={'ExpectedPurchaseFor_6_Months': 'Predicged No of Purchases in 6 Months'}, inplace=True)
    output_df.rename(columns={'6_Months_CLV': 'Predicted CLV In 6 Months'}, inplace=True)



    # Save the processed DataFrame to a new Excel file
    output_file = 'output.xlsx'
    output_df.to_excel(output_file, index=True)  

    return render_template('display.html', tables=[output_df.to_html(classes='data', index=False)], output_file=output_file)

@app.route('/download/<filename>')
def download_file(filename):
    return send_file(filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
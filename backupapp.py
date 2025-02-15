from flask import Flask, request, render_template, jsonify, send_file
import pandas as pd
import os
from lifetimevalue import lv  # Replace with actual import if using a specific library

app = Flask(__name__)

# Route for the main page
@app.route('/')
def index():
    return render_template('index.html')  # Your main upload and filter page

# Route for processing data
@app.route('/process', methods=['POST'])
def process_data():
    # Check if the post request has the file part
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    
    # If the user does not select a file, the browser submits an empty file without a filename
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    # Process the uploaded Excel file
    df = pd.read_excel(file)  # Load the file directly from the uploaded content

    # Get filter values from the request
    filter1 = request.form.get('filter1')
    filter2 = request.form.get('filter2')
    filter3 = request.form.get('filter3')

    # Validate filters
    if filter1 == filter2:
        return jsonify({"error": "Filter 1 and Filter 2 cannot be the same."}), 400

    # Perform data processing based on filters (this is example logic)
    output_df = lv(df, filter1, filter2, filter3)  # Replace with your processing function

    aggregated_df = df.groupby('CustomerID', as_index=False).first()

    output_df = pd.merge(output_df, aggregated_df[['CustomerID', 'CustomerName']], on='CustomerID', how='left')

    output_df['frequency'] = output_df['frequency'].astype(int)
    output_df['recency'] = output_df['recency'].astype(int)
    output_df['T'] = output_df['T'].astype(int)
    output_df['PredictedNoOfOrders'] = output_df['ExpectedPurchaseFor_6_Months'].astype(int)
    output_df['PredictedCLV'] = output_df['6_Months_CLV'].astype(int)
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

    # Save processed data to a temporary Excel file
    output_file = 'output.xlsx'
    output_df.to_excel(output_file, index=False)

    # Return the output file and metrics
    return jsonify({
        "message": "Data processed successfully!",
        "output_file": output_file,
        "filter1": filter1,
        "filter2": filter2,
        "filter3": filter3,
        "metric1": output_df['Column1'].sum(),  # Replace with actual metric calculation
        "metric2": output_df['Column2'].mean(),  # Replace with actual metric calculation
        "metric3": output_df['Column3'].max()    # Replace with actual metric calculation
    })

# Route to display processed data
@app.route('/display/<filename>')
def display_data(filename):
    df = pd.read_excel(filename)  # Load the processed data

    # Replace these with your actual metric calculations if needed
    metric1 = df['Column1'].sum()  # Example metric
    metric2 = df['Column2'].mean()  # Example metric
    metric3 = df['Column3'].max()   # Example metric

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
import streamlit as st
import pandas as pd
import re
from snowflake.snowpark import Session
from snowflake.core import Root
from PIL import Image
from landingai.predict import Predictor

### Connection to Snowflake and get Cortex Search Service from Root(session).
session = Session.builder.configs(st.secrets["connections"]["snowflake"]).getOrCreate()

## API Info
api_info = session.table("IMG_RECG.API_CREDENTIALS").to_pandas()
api_key_euph = api_info[api_info["NAME"]=="LANDINGAI"]["API_KEY"].values[0]
endpoint_id = api_info[api_info["NAME"]=="LANDINGAI"]["ENDPOINT_ID"].values[0]
api_key = None

## Transaction Info
tran_info = session.table("IMG_RECG.TRANSACTION").to_pandas()

if __name__ == "__main__":
  ### Set page layout
  st.set_page_config(layout="wide")

  ### Side Bar
  with st.sidebar:
    # API KEY
    api_key = st.text_input("API KEY", value = "land_sk_6oldW5HSsjgXSwxyQf0vZfDwOR2uOIaGoleFfIk1TLSyIG19", type = "password") #bt

    # Upload area
    uploaded_file = st.file_uploader("📂 Choose a file")
    predictions = None

    # When file is uploaded:
    if uploaded_file and api_key:
      # To read file as bytes:
      bytes_data = uploaded_file.getvalue()

      # Upload the image:
      imagefile = Image.open(uploaded_file)

      if api_key:
      # Send to model for prediction,
        predictor = Predictor(endpoint_id, api_key=api_key)
        predictions = predictor.predict(imagefile) #ObjectDetectionPrediction Object

      # Predict the result
      with st.expander("📰 Returned result:"):
        st.json(predictions)

  ### Main Top Area:
  st.header("🛒 POC demo - Conceptual Idea")
  st.caption("🖌️ Created by Euphemia")

  with st.expander("🛍️ Shopping Transactions"):
    st.dataframe(tran_info)

  ### Main columns layout
  maincol1, maincol2 = st.columns([1, 2])

  ### Main Left Column
  with maincol1:
    st.subheader("🖼️ Uploaded Image")
    if uploaded_file:
      st.image(uploaded_file)

  ### Main Right Column
  with maincol2:
    st.subheader("🗃️ Show Results")

    # Prep for the results
    count = 1
    list_predicted_items = []

    if predictions:
      # Loop thru all the predictions
      for each in predictions:
        item_name = each.label_name
        df_item = None
        last_time_purchase = None

        if item_name not in list_predicted_items:
          # Show each item:
          with st.expander(f"{count} : {item_name}"):
            df_item = tran_info[tran_info["ITEM"].str.contains(item_name, case = False)]
            # Show dataset:
            st.dataframe(df_item)

          list_predicted_items.append(item_name)
          count += 1
        
        if df_item.size:
          #Latest purchase
          latest_purchase = df_item[df_item["TRANSACTION_TIMESTAMP"]==df_item["TRANSACTION_TIMESTAMP"].max()]

          datets = latest_purchase["TRANSACTION_TIMESTAMP"].values[0]
          store = latest_purchase["MERCHANT_NAME"].values[0]
          amount = latest_purchase["AMOUNT"].values[0]

          st.write(f"The most recent purchase of {item_name} is at {store} at {datets} for {amount}.")
          st.button("Would you like to purchase at the same store again?")
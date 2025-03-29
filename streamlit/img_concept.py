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

## Transaction Info
tran_info = session.table("IMG_RECG.TRANSACTION").to_pandas()

if __name__ == "__main__":
  ### Set page layout
  st.set_page_config(layout="wide")

  ### Side Bar
  with st.sidebar:
    # API KEY
    api_key = st.text_input("API KEY") #api_key_euph

    # Upload area
    uploaded_file = st.file_uploader("📂 Choose a file")
    predictions = None

    # When file is uploaded:
    if uploaded_file and api_key:
      # To read file as bytes:
      bytes_data = uploaded_file.getvalue()

      # Upload the image:
      imagefile = Image.open(uploaded_file)

      # Send to model for prediction,
      predictor = Predictor(endpoint_id, api_key=api_key)
      predictions = predictor.predict(imagefile) #ObjectDetectionPrediction Object

      # Predict the result
      with st.expander("📰 Returned result:"):
        st.json(predictions)

  ### Main Top Area:
  st.header("🛒 POC demo - Shopping Conceptual Idea")
  st.caption("🖌️ Created by Euphemia")

  with st.expander("🛍️ Shopping Transactions"):
    st.dataframe(tran_info)

  ### Main columns layout
  maincol1, maincol2 = st.columns([1, 2])

  ### Main Left Column
  with maincol1:
    if uploaded_file:
      st.subheader("🖼️ Uploaded Image")
      st.image(uploaded_file)

  ### Main Right Column
  with maincol2:
    if predictions:
      #for each in predictions:
        #st.write(each)
      st.subheader("🗃️ Show Results")
      count = 1
      list_predicted_items = []

      for each in predictions:
        item = each.label_name
        if item not in list_predicted_items:
          # Show each item:
          with st.expander(f"{count} : {item}"):
            # Show dataset:
            st.dataframe(tran_info[tran_info["ITEM"].str.contains(item, case = False)])

          list_predicted_items.append(item)
          count += 1

    
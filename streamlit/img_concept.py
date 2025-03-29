import streamlit as st
import pandas as pd
import requests
from snowflake.snowpark import Session
from snowflake.core import Root
from PIL import Image
from landingai.predict import Predictor

### Connection to Snowflake and get Cortex Search Service from Root(session).
session = Session.builder.configs(st.secrets["connections"]["snowflake"]).getOrCreate()

api_info = session.table("IMG_RECG.API_CREDENTIALS").to_pandas()
api_key = api_info[api_info["NAME"]=="LANDINGAI"]["API_KEY"].values[0]
endpoint_id = api_info[api_info["NAME"]=="LANDINGAI"]["ENDPOINT_ID"].values[0]

tran_info = session.table("IMG_RECG.TRANSACTION").to_pandas()

if __name__ == "__main__":
  ### Set page layout
  st.set_page_config(layout="wide")

  ### Side Bar
  with st.sidebar:
    uploaded_file = st.file_uploader("Choose a file")
    predicted_results = None
    if uploaded_file:
      # To read file as bytes:
      bytes_data = uploaded_file.getvalue()

      # Upload the image:
      imagefile = Image.open(uploaded_file)

      # Send to model for prediction,
      predictor = Predictor(endpoint_id, api_key=api_key)
      predictions = predictor.predict(imagefile)

      # Predict the result
      predicted_results = st.json(predictions)

  ### Main Page Prep:
  maincol1, maincol2 = st.columns(2)

  ### Main Top Area:
  st.header("🛒 POC demo - Shopping Conceptual Idea")
  st.caption("Created by Euphemia")

  with st.expander:
    st.dataframe(tran_info)

  ### Main Left Column
  with maincol1:
    if uploaded_file:
      st.image(uploaded_file)

  ### Main Right Column
  with maincol2:
    if predicted_results:
      st.write(predicted_results[0])
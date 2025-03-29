import streamlit as st
from snowflake.snowpark import Session
from snowflake.core import Root
import pandas as pd
import requests
from PIL import Image
from landingai.predict import Predictor

### Connection to Snowflake and get Cortex Search Service from Root(session).
session = Session.builder.configs(st.secrets["connections"]["snowflake"]).getOrCreate()

api_info = session.table("IMG_RECG.API_CREDENTIALS").to_pandas()
api_key = api_info[api_info["NAME"]=="LANDINGAI"]["API_KEY"].values[0]
endpoint_id = api_info[api_info["NAME"]=="LANDINGAI"]["ENDPOINT_ID"].values[0]

if __name__ == "__main__":
   ### Set page layout
   st.set_page_config(layout="wide")

   st.write("Hello")

   uploaded_file = st.file_uploader("Choose a file")
   if uploaded_file is not None:
     # To read file as bytes:
     bytes_data = uploaded_file.getvalue()

     imagefile = Image.open(uploaded_file)

     pedictor = Predictor(endpoint_id, api_key=api_key)
     predictions = predictor.predict(imagefile)
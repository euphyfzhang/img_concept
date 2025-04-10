import streamlit as st
import pandas as pd
import re
from snowflake.snowpark import Session
from snowflake.core import Root
from PIL import Image
from landingai.predict import Predictor

# service parameters
CORTEX_SEARCH_DATABASE = "RESUME_AI_DB"
CORTEX_SEARCH_SCHEMA = "IMG_RECG"
CORTEX_SEARCH_SERVICE = "CS_ANALYST"

# Each path points to a YAML file defining a semantic model
AVAILABLE_SEMANTIC_MODELS_PATHS = [
    f"{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.INSTAGE/SEMANTIC_FILE/semantic_analyst_file.yaml"
]
API_ENDPOINT = "/api/v2/cortex/analyst/message"
FEEDBACK_API_ENDPOINT = "/api/v2/cortex/analyst/feedback"
API_TIMEOUT = 50000  # in milliseconds

### Connection to Snowflake and get Cortex Search Service from Root(session).
session = Session.builder.configs(st.secrets["connections"]["snowflake"]).getOrCreate()

## Chatbot related objects
root = Root(session)
svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

## API Info
api_info = session.table("IMG_RECG.API_CREDENTIALS").to_pandas()
landingai_api = api_info[api_info["NAME"]=="LANDINGAI"]
api_key_euph = landingai_api["API_KEY"].values[0]
endpoint_id = landingai_api["ENDPOINT_ID"].values[0]
api_key = None

## Website contents
images_path = "@IMG_RECG.INSTAGE"
website_imgs = session.table("IMG_RECG.WEBSITE_IMAGES").to_pandas()
banner_loc = website_imgs[website_imgs["DESCRIPTION"]=="BANNER"]["IMAGE_NAME"].values[0]
banner_image = session.file.get_stream(f"{images_path}/BANNER/{banner_loc}" , decompress=False).read()

## Transaction Info
tran_info = session.table("IMG_RECG.TRANSACTION").to_pandas()

if __name__ == "__main__":
  ### Set page layout
  st.set_page_config(layout="wide")
  err_message = None

  ### Side Bar
  with st.sidebar:
    # API KEY
    api_key = st.text_input("API KEY", type = "password")

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
        try:
          # Send to model for prediction,
          predictor = Predictor(endpoint_id, api_key=api_key)
          predictions = predictor.predict(imagefile) #ObjectDetectionPrediction Object
        except Exception as e:
          err_message = e.message

      # Predict the result
      with st.expander("📰 Returned result:"):
        st.json(predictions)

  ### Main Top Area:
  ### The banner
  st.image(banner_image, width = 1400)

  ### The 1st section in MAIN PAGE
  with st.expander("🛍️ Shopping Transactions"):
    st.dataframe(tran_info)

  ### Main columns layout
  maincol1, maincol2 = st.columns([1, 2])

  ### The 2nd section in MAIN PAGE
  with st.container(height=400, border=False):
    ### Main Left Column
    with maincol1:
      st.subheader("🖼️ Uploaded Image")
      if uploaded_file:
        st.image(uploaded_file)

    ### Main Right Column
    with maincol2:
      st.subheader("🗃️ Show Results")

      ## Prep for the results
      count = 1
      list_predicted_items = []

      if err_message and "UNAUTHORIZED" in err_message:
        st.warning("Please check whether the **API KEY** has been input correctly. Thanks.", icon="⚠️")
        st.caption("Note : **API KEY** is located at the top of the sidebar.")
      else:
        pass

      if predictions:
        ## Loop thru all the predictions
        for each in predictions:
          item_name = each.label_name
          df_item = None
          last_time_purchase = None

          if item_name not in list_predicted_items:
            ## Show each item:
            with st.expander(f"{count} : {item_name}"):
              df_item = tran_info[tran_info["ITEM"].str.contains(item_name, case = False)]
              
              if len(df_item):
                ## Show dataset:
                st.dataframe(df_item)

                ## Latest purchase
                latest_purchase = df_item[df_item["TRANSACTION_TIMESTAMP"]==df_item["TRANSACTION_TIMESTAMP"].max()]

                datets = latest_purchase["TRANSACTION_TIMESTAMP"].values[0]
                store = latest_purchase["MERCHANT_NAME"].values[0]
                amount = latest_purchase["AMOUNT"].values[0]
                
                st.markdown(f"The most recent purchase of :blue-background[{item_name}] is at :blue[{store}] at :orange-badge[{datets}] for :red[${amount}].")
                #st.button("Re-purchase?")

            list_predicted_items.append(item_name)
            count += 1
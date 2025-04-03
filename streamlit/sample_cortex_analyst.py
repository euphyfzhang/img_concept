# Public Docs: https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/rest-api/cortex-analyst

import json, re, copy
from typing import Any, Generator, Iterator

import pandas
import pandas as pd
import requests
from snowflake.snowpark import Session
import streamlit as st

from PIL import Image
from landingai.predict import Predictor

DATABASE = "RESUME_AI_DB"
SCHEMA = "IMG_RECG"
STAGE = "INSTAGE"
FILE = "SEMANTIC_FILE/semantic_analyst_file.yaml"
SEMANTIC_FILE = f"{DATABASE}.{SCHEMA}.{STAGE}/{FILE}"
AVAILABLE_SEMANTIC_MODELS_PATHS = f"{DATABASE}.{SCHEMA}.{STAGE}/{FILE}"

session = Session.builder.configs(st.secrets["connections"]["snowflake"]).getOrCreate()
st.session_state.CONN = session.connection

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

## Transaction data
tran_info = session.table("IMG_RECG.TRANSACTION").to_pandas()

##
err_message = None
list_predicted_items = []

def reset_session_state():
    """Reset important session state elements."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion
    st.session_state.warnings = []  # List to store warnings
    st.session_state.form_submitted = (
        {}
    )  # Dictionary to store feedback submission for each request


def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occured!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False

def computer_vision_prediction(image_file, api_key=""):
    results = []

    # Upload the image:
    imagefile = Image.open(image_file)

    if api_key:
        try:
            # Send to model for prediction,
            predictor = Predictor(endpoint_id, api_key=api_key)
            predictions = predictor.predict(imagefile) #ObjectDetectionPrediction Object

            for each in predictions:
                results.append({"status" : "SUCCESS", "item" : each.label_name})

        except Exception as e:
            err_message = str(e)
            results.append({"status" : "FAILURE", "error_message" : err_message})
    
    return results


def process_user_input(prompt, api_key = ""):
    """
    Process user input and update the conversation history.

    Args:
        prompt (str): The user's input.
    """
    # Clear previous warnings at the start of a new request
    st.session_state.warnings = []

    # Create a new message, append to history and display imidiately
    new_user_message = None

    # If prompt is just text, no file attached:
    if type(prompt) == str:
        new_user_message = {
                        "role": "user",
                        "content": [{"type": "text", "text": prompt}]
                        }
    # If prompt is a file attached:
    else:
        new_user_message = {
                            "role": "user",
                            "content": [{"type": "text", "text": prompt.text}]
                            }
        
        if prompt["files"]:
            new_user_message["content"].append({"type": "image", "image": prompt["files"][0]})

            # Send to Computer Vision Tool for prediction.
            predicted_item = computer_vision_prediction(prompt["files"][0], api_key=api_key)

            if predicted_item[0]["status"] == "SUCCESS":
                for each in new_user_message["content"]:
                    if each["type"] == "text":
                        each["text"] = each["text"] + f"( for the item **:red[{predicted_item[0]["item"]}]** )"

    st.session_state.messages.append(new_user_message)

    with st.chat_message("user"):
        user_msg_index = len(st.session_state.messages) - 1
        display_message(new_user_message["content"], user_msg_index)

    # Show progress indicator inside analyst chat message while waiting for response
    with st.chat_message("analyst"):
        with st.spinner("Waiting for Analyst's response..."):

            text_messages = copy.deepcopy(st.session_state.messages)

            for each in text_messages:
                each["content"] = list(filter(lambda x: x["type"] == "text", each["content"]))

            response, request_id, error_msg = get_analyst_response(text_messages)
            #st.write(response)

            analyst_message = {
                    "role": "analyst",
                    "content": response,
                    "request_id": request_id,
                }

            if error_msg:
                st.session_state["fire_API_error_notify"] = True

            st.session_state.messages.append(analyst_message)
            st.rerun()


def display_warnings():
    """
    Display warnings to the user.
    """
    warnings = st.session_state.warnings
    for warning in warnings:
        st.warning(warning["message"], icon="⚠️")

def parsed_response_message(content):

    response_string = content.decode("utf-8")
    removed_charactor = re.sub(r"event: [\s\w\n.:]*", "", response_string)
    cleaned_reponse = removed_charactor.split("\n")
    #debug purpose
    parsed_list = []
    error_message = None

    for each in cleaned_reponse:
        if each:
            try:
                parsed_list.append(json.loads(each))
            except Exception as e:
                error_message = str(e)

    text_delta = []
    suggestions_delta = []
    messages = []
    request_id = None
    error_code = None
    request_id = None
    sql = None
    confidence = None
    other = None

    for each in parsed_list:
        if "text_delta" in each:
            text_delta.append(each["text_delta"])
        elif "suggestions_delta" in each:
            suggestions_delta.append(each["suggestions_delta"])
        elif "status" in each:
            request_id = each["request_id"]
        elif "message" in each:
            messages.append(each["message"])
        elif "error_code" in each:
            error_code = each["error_code"]
        elif "request_id" in each:
            request_id = each["request_id"]
        elif "type" in each and "sql" in each["type"]:
            sql = each["statement_delta"]
            confidence = each["confidence"]
        #else:
            #sql = each

    if error_message:
        text = error_message
    else:
        text = "".join(text_delta)

    rebuilt_response = [{ "type" : "text", "text" : text}
                        , {"type" : "suggestion", "suggestions" : suggestions_delta}
                        , {"type" : "status", "messages" : messages, "error_code" : error_code}
                        , {"type" : "sql", "sql": sql, "confidence" : confidence}
                        , {"type" : "request_id", "request_id": request_id}
                        ]
    
    #st.header(rebuilt_response)

    return rebuilt_response, request_id, error_message


def get_analyst_response(messages):
    """
    Send chat history to the Cortex Analyst API and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    #st.write(f"session_state.message: {st.session_state.messages}")
    # Prepare the request body with the user's prompt

    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{SEMANTIC_FILE}",
        "stream": True,
    }

    # Send a POST request to the Cortex Analyst API endpoint
    # Adjusted to use positional arguments as per the API's requirement
    resp = requests.post(
        url=f"https://{st.session_state.CONN.host}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
        stream=True,
    )

    # Content is a string with serialized JSON object
    parsed_content, request_id, error_message = parsed_response_message(resp.content)

    return parsed_content, request_id, error_message



def display_message(content, message_index, request_id=""):
    """
    Display a single message content.

    """
    #For debug purpose
    #st.subheader(content)

    for item in content:
        match item["type"]:
            case "text":
                st.markdown(item["text"])
            
            case "image":
                st.image(item["image"], width = 200)

            case "suggestion":
                # Consolidate the suggestion_delta (word pieces) into a complete sentence.
                suggestions = {}
                for each in item["suggestions"]:
                    idx = each["index"]
                    if idx in suggestions:
                        suggestions.update({idx:suggestions[idx] + each["suggestion_delta"]})
                    else:
                        suggestions[idx] = each["suggestion_delta"]

                # Display suggestions as buttons
                for key, value in suggestions.items():
                    if st.button(value, key=f"suggestion_{message_index}_{key}"):
                        st.session_state.active_suggestion = value

            case "sql":
                # Display the SQL query and results
                if item["sql"]:
                    display_sql_query(
                        item["sql"], message_index, item["confidence"], request_id
                    )


@st.cache_data(show_spinner=False)
def get_query_exec_result(query):
    """
    Execute the SQL query and convert the results to a pandas DataFrame.

    Args:
        query (str): The SQL query.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[str]]: The query results and the error message.
    """
    global session
    try:
        df = session.sql(query).to_pandas()
        return df, None
    except SnowparkSQLException as e:
        return None, str(e)


def display_sql_confidence(confidence):
    if confidence is None:
        return
    verified_query_used = confidence["verified_query_used"]
    with st.popover(
        "Verified Query Used",
        help="The verified query from [Verified Query Repository](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/verified-query-repository), used to generate the SQL",
    ):
        with st.container():
            if verified_query_used is None:
                st.text(
                    "There is no query from the Verified Query Repository used to generate this SQL answer"
                )
                return
            #st.text(f"Name: {verified_query_used['name']}")
            st.text(f"Question: {verified_query_used['question']}")
            #st.text(f"Verified by: {verified_query_used['verified_by']}")
            #st.text(
            #    f"Verified at: {datetime.fromtimestamp(verified_query_used['verified_at'])}"
            #)
            st.text("SQL query:")
            st.code(verified_query_used["sql"], language="sql", wrap_lines=True)


def display_sql_query(sql, message_index, confidence, request_id):
    """
    Executes the SQL query and displays the results in form of data frame and charts.

    Args:
        sql (str): The SQL query.
        message_index (int): The index of the message.
        confidence (dict): The confidence information of SQL query generation
        request_id (str): Request id from user request
    """

    # Display the SQL query
    with st.expander("SQL Query", expanded=False):
        st.code(sql, language="sql")
        display_sql_confidence(confidence)

    # Display the results of the SQL query
    with st.expander("Results", expanded=True):
        with st.spinner("Running SQL..."):
            df, err_msg = get_query_exec_result(sql)
            if df is None:
                st.error(f"Could not execute generated SQL query. Error: {err_msg}")
            elif df.empty:
                st.write("Query returned no data")
            else:
                # Show query results in two tabs
                data_tab, chart_tab = st.tabs(["Data 📄", "Chart 📉"])
                with data_tab:
                    st.dataframe(df, use_container_width=True)

                with chart_tab:
                    display_charts_tab(df, message_index)
    #if request_id:
        #display_feedback_section(request_id)


def display_charts_tab(df, message_index):
    """
    Display the charts tab.

    Args:
        df (pd.DataFrame): The query results.
        message_index (int): The index of the message.
    """
    # There should be at least 2 columns to draw charts
    if len(df.columns) >= 2:
        all_cols_set = set(df.columns)
        col1, col2 = st.columns(2)
        x_col = col1.selectbox(
            "X axis", all_cols_set, key=f"x_col_select_{message_index}"
        )
        y_col = col2.selectbox(
            "Y axis",
            all_cols_set.difference({x_col}),
            key=f"y_col_select_{message_index}",
        )
        chart_type = st.selectbox(
            "Select chart type",
            options=["Line Chart 📈", "Bar Chart 📊"],
            key=f"chart_type_{message_index}",
        )
        if chart_type == "Line Chart 📈":
            st.line_chart(df.set_index(x_col)[y_col])
        elif chart_type == "Bar Chart 📊":
            st.bar_chart(df.set_index(x_col)[y_col])
    else:
        st.write("At least 2 columns are required")


def display_feedback_section(request_id):
    with st.popover("📝 Query Feedback"):
        if request_id not in st.session_state.form_submitted:
            with st.form(f"feedback_form_{request_id}", clear_on_submit=True):
                positive = st.radio(
                    "Rate the generated SQL", options=["👍", "👎"], horizontal=True
                )
                positive = positive == "👍"
                submit_disabled = (
                    request_id in st.session_state.form_submitted
                    and st.session_state.form_submitted[request_id]
                )

                #feedback_message = st.text_input("Optional feedback message")
                #submitted = st.form_submit_button("Submit", disabled=submit_disabled)
                #if submitted:
                    #err_msg = submit_feedback(request_id, positive, feedback_message)
                    #st.session_state.form_submitted[request_id] = {"error": err_msg}
                    #st.session_state.popover_open = False
                    #st.rerun()
        elif (
            request_id in st.session_state.form_submitted
            and st.session_state.form_submitted[request_id]["error"] is None
        ):
            st.success("Feedback submitted", icon="✅")
        else:
            st.error(st.session_state.form_submitted[request_id]["error"])


def submit_feedback(request_id, positive, feedback_message):
    request_body = {
        "request_id": request_id,
        "positive": positive,
        "feedback_message": feedback_message,
    }
    resp = _snowflake.send_snow_api_request(
        "POST",  # method
        FEEDBACK_API_ENDPOINT,  # path
        {},  # headers
        {},  # params
        request_body,  # body
        None,  # request_guid
        API_TIMEOUT,  # timeout in milliseconds
    )

    if resp["status"] == 200:
        return None

    parsed_content = json.loads(resp["content"])
    # Craft readable error message
    err_msg = f"""
        #🚨 An Analyst API error has occurred 🚨
        
        * response code: `{resp['status']}`
        * request-id: `{parsed_content['request_id']}`
        * error code: `{parsed_content['error_code']}`
        
        #Message:
        ```
        {parsed_content['message']}
        ```
        """
    return err_msg

if __name__ == "__main__":
    # Initialize session state
    if "messages" not in st.session_state:
        reset_session_state()
    
    ### HEADER AREA
    st.set_page_config(layout="centered"
                        , page_title="SnapLedger"
                        , page_icon="🍭"
                        , initial_sidebar_state="expanded")
    # Set the title and introductory text of the app
    with st.container(border = False):
        st.image(banner_image, width = 700, caption = "by Euphemia (2025.03)")

    with st.expander("🛒 Shopping Transactions"):
        st.dataframe(tran_info)

    ### SIDEBAR AREA
    with st.sidebar:
        # Center this button
        _, btn_container, _ = st.columns([2, 6, 2])
        if btn_container.button("🗑️ Clear Chat History", use_container_width=True):
            reset_session_state()

        st.selectbox(
            "Selected semantic model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )

        st.divider()

        ## API KEY
        api_key = st.text_input("🔑 API Key", type = "password")

    ### CHAT DISPLAY
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            if role == "analyst":
                display_message(content, idx, message["request_id"])
            else:
                display_message(content, idx)

    ### CHAT AREA
    if err_message:
        st.warning(err_message, icon = "💥")

    # Handle chat input
    user_input = st.chat_input("What are you looking up?"
                            , accept_file=True
                            , file_type=["jpg", "jpeg", "png"]
                            )

    if user_input:
        process_user_input(user_input, api_key)

    # Handle suggested question click
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)

    handle_error_notifications()

# Public Docs: https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/rest-api/cortex-analyst

import time, json, re, copy, yaml
import pandas as pd
import requests
import streamlit as st
from PIL import Image
from snowflake.snowpark import Session
from landingai.predict import Predictor

### Release info
release_version = "Release-1.1.1 [2025-04-05]"

### Open config.yaml file.
with open("streamlit/config.yaml", "r") as file:
    config = yaml.safe_load(file)

### Configurations
SEMANTIC_FILE = f"{config["snowflake"]["database"]}.{config["snowflake"]["schema"]}.{config["snowflake"]["stage"]}/{config["snowflake"]["semantic_analyst_file"]}"
CORTEX_SEARCH_SERVICE = f"{config["snowflake"]["database"]}.{config["snowflake"]["schema"]}.{config["snowflake"]["cortex_search_service"]}"

### Snowflake connection
session = Session.builder.configs(st.secrets["connections"]["snowflake"]).getOrCreate()
st.session_state.CONN = session.connection

## API Info
landingai_api = "https://api.landing.ai/v1/projects/"
endpoint_id = config["endpoint"]["landingai"]
api_key = st.secrets["LandingAI_key"]

## Website contents
images_path = "@IMG_RECG.INSTAGE"
website_imgs = session.table("IMG_RECG.WEBSITE_IMAGES").to_pandas()
banner_loc = website_imgs[website_imgs["DESCRIPTION"]=="BANNER"]["IMAGE_NAME"].values[0]
banner_image = session.file.get_stream(f"{images_path}/BANNER/{banner_loc}" , decompress=False).read()

## Transaction data
tran_info = session.table("IMG_RECG.TRANSACTION").to_pandas()

## ERROR
err_message = None

list_predicted_items = []

def reset_session_state():
    """Reset important session state elements."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion
    st.session_state.warnings = []  # List to store warnings
    st.session_state.form_submitted = ({})  # Dictionary to store feedback submission for each request

def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occured!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False

def cortex_agent_call(message, limit = 10):

    cleansed_message = message[-1]["content"][0]["text"]

    request_body = {
        "model": "llama3.1-70b",
        "response_instruction" : "You have a name, which is 'Aime'. You always respond with a postive",
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": cleansed_message
                    }
                ]
            }
        ],
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "analyst1"
                }
            },
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": "search1"
                }
            }
        ],
        "tool_resources": {
            "analyst1": {"semantic_model_file": f"@{SEMANTIC_FILE}"},
            "search1": {
                "name": CORTEX_SEARCH_SERVICE,
                "max_results": limit,
                "id_column": "product_dimension"
            }
        }
    }
    
    try:
        resp = requests.post(
                    url=f"https://{st.session_state.CONN.host}{config["endpoint"]["cortex_agent"]}",
                    json=request_body,
                    headers={
                        "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
                        "Content-Type": "application/json",
                    },
                )

        if resp.status_code != 200:
            raise Exception(f"API call failed with status code {resp.status_code}")
        
        response_content, request_id, error_message = parsed_response_message(resp.content, "agent")

        return response_content, request_id, error_message

    except json.JSONDecodeError as e:
            st.error("❌ Failed to parse API response. The server may have returned an invalid JSON format.")
            st.error(f"Raw response: {str(e)}")
            return None

    except Exception as e:
        st.error(f"Error making request: {str(e)}")
        return None

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
    with st.chat_message("assistant"):
        with st.spinner(" Aime the bot assistant is responding...	💬"):

            text_messages = copy.deepcopy(st.session_state.messages)

            for each in text_messages:
                each["content"] = list(filter(lambda x: x["type"] == "text", each["content"]))

            response, request_id, error_msg = cortex_agent_call(text_messages) #get_analyst_response(text_messages)
            #st.write(response)

            analyst_message = {
                    "role": "assistant",
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

def parsed_response_message(content, cortex_type):

    response_string = content.decode("utf-8")
    removed_charactor = re.sub(r"event: [\s\w\n.:]*", "", response_string)
    cleaned_response = removed_charactor.split("\n")
    session.sql(f"INSERT INTO RESUME_AI_DB.IMG_RECG.LOG(MESSAGE) VALUES ('{"#".join(cleaned_response)}');").collect()

    wanted_response = []
    parsed_list = []
    error_message = None

    text = []
    sql = None
    suggestions = []
    request_id = str(time.time())
    
    if cortex_type == "agent":
        for each_response in cleaned_response:
            if each_response:
                try:
                    wanted_response.append(json.loads(each_response))
                except Exception as e:
                    pass

        #for debug
        #session.sql(f"INSERT INTO RESUME_AI_DB.IMG_RECG.LOG(MESSAGE) VALUES ('{some_variable)}');").collect()
        
        for each_response in wanted_response:
            delta_content = each_response["delta"]["content"]
            for each in delta_content:
                if each:
                    try:
                        if "text" in each:
                            parsed_list.append(each)
                            
                        elif "tool_results" in each:
                            tool_results_content = each["tool_results"]["content"]
                            for sub_each in tool_results_content:
                                parsed_list.append(sub_each["json"])
                    except Exception as e:
                        error_message = str(e)

        for each in parsed_list:
            if "suggestions" in each:
                suggestions.append(each["suggestions"])

            if "sql" in each:
                sql = each["sql"]

            if "text" in each:
                text.append(each["text"])

        rebuilt_response = [{ "type" : "text", "text" : "".join(text)}
                            , {"type" : "suggestion", "suggestions" : suggestions}
                            , {"type" : "sql", "sql": sql}
                            , {"type" : "request_id", "request_id": request_id}
                            ]

    elif cortex_type == "analyst":

        text_delta = []
        suggestions_delta = []
        messages = []
        error_code = None
        request_id = None
        sql = None
        confidence = None
        other = None

        for each in cleaned_response:
            if each:
                try:
                    parsed_list.append(json.loads(each))
                except Exception as e:
                    error_message = str(e)

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

    return rebuilt_response, request_id, error_message


def get_analyst_response(messages):
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{SEMANTIC_FILE}",
        "stream": True,
    }

    # Send a POST request to the Cortex Analyst API endpoint
    # Adjusted to use positional arguments as per the API's requirement
    resp = requests.post(
        url=f"https://{st.session_state.CONN.host}{config["endpoint"]["cortex_analyst_message"]}",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
        stream=True,
    )

    # Content is a string with serialized JSON object
    parsed_content, request_id, error_message = parsed_response_message(resp.content, "analyst")

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
                    if "index" in each:
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
                    if "confidence" in item:
                        display_sql_query(item["sql"], message_index, item["confidence"], request_id)
                    else:
                        display_sql_query(
                            item["sql"], message_index, "", request_id)



@st.cache_data(show_spinner=False)
def get_query_exec_result(query):
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

            st.text(f"Question: {verified_query_used['question']}")
            st.text(f"Verified by: {verified_query_used['verified_by']}")
            #st.text(f"Verified at: {datetime.fromtimestamp(verified_query_used['verified_at'])}")
            st.text("SQL query:")
            st.code(verified_query_used["sql"], language="sql", wrap_lines=True)


def display_sql_query(sql, message_index, confidence, request_id):

    # Display the SQL query
    with st.expander("SQL Query", expanded=False):
        st.code(sql, language="sql")
        if confidence:
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
    if request_id:
        display_feedback_section(request_id)


def display_charts_tab(df, message_index):

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

                feedback_message = st.text_input("Optional feedback message")
                submitted = st.form_submit_button("Submit", disabled=submit_disabled)
                if submitted:
                    err_msg = submit_feedback(request_id, positive, feedback_message)
                    st.session_state.form_submitted[request_id] = {"error": err_msg}
                    st.session_state.popover_open = False
                    st.rerun()
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

    resp = requests.post(
        url=f"https://{st.session_state.CONN.host}{config["endpoint"]["cortex_analyst_feedback"]}",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
        stream=True,
    )

    if resp.status_code == 200:
        return None

    parsed_content = json.loads(resp.content)
    # Craft readable error message
    err_msg = f"""
        #🚨 An Analyst API error has occurred 🚨
        
        * response code: `{resp.status_code}`
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
        st.image(banner_image, width = 700, caption = release_version)

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
            SEMANTIC_FILE,
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
            if role == "assistant":
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

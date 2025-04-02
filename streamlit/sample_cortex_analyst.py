# Public Docs: https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/rest-api/cortex-analyst

import json
import re
from typing import Any, Generator, Iterator

import pandas
import pandas as pd
import requests
from snowflake.snowpark import Session
import streamlit as st

DATABASE = "RESUME_AI_DB"
SCHEMA = "IMG_RECG"
STAGE = "INSTAGE"
FILE = "SEMANTIC_FILE/semantic_analyst_file.yaml"
SEMANTIC_FILE = f"{DATABASE}.{SCHEMA}.{STAGE}/{FILE}"
AVAILABLE_SEMANTIC_MODELS_PATHS = f"{DATABASE}.{SCHEMA}.{STAGE}/{FILE}"

session = Session.builder.configs(st.secrets["connections"]["snowflake"]).getOrCreate()
st.session_state.CONN = session.connection

def main():
    # Initialize session state
    if "messages" not in st.session_state:
        reset_session_state()
    show_header_and_sidebar()
    display_conversation()
    handle_user_inputs()
    handle_error_notifications()


def reset_session_state():
    """Reset important session state elements."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion
    st.session_state.warnings = []  # List to store warnings
    st.session_state.form_submitted = (
        {}
    )  # Dictionary to store feedback submission for each request


def show_header_and_sidebar():
    """Display the header and sidebar of the app."""
    # Set the title and introductory text of the app
    st.title("Cortex Analyst")
    st.markdown(
        "Ask the analyst! "
    )

    # Sidebar with a reset button
    with st.sidebar:
        st.selectbox(
            "Selected semantic model:",
            AVAILABLE_SEMANTIC_MODELS_PATHS,
            format_func=lambda s: s.split("/")[-1],
            key="selected_semantic_model_path",
            on_change=reset_session_state,
        )
        st.divider()
        # Center this button
        _, btn_container, _ = st.columns([2, 6, 2])
        if btn_container.button("Clear Chat History", use_container_width=True):
            reset_session_state()


def handle_user_inputs():
    """Handle user inputs from the chat interface."""
    # Handle chat input
    user_input = st.chat_input("What is your question?")
    if user_input:
        process_user_input(user_input)
    # Handle suggested question click
    elif st.session_state.active_suggestion is not None:
        suggestion = st.session_state.active_suggestion
        st.session_state.active_suggestion = None
        process_user_input(suggestion)


def handle_error_notifications():
    if st.session_state.get("fire_API_error_notify"):
        st.toast("An API error has occured!", icon="🚨")
        st.session_state["fire_API_error_notify"] = False


def process_user_input(prompt):
    """
    Process user input and update the conversation history.

    Args:
        prompt (str): The user's input.
    """
    # Clear previous warnings at the start of a new request
    st.session_state.warnings = []

    # Create a new message, append to history and display imidiately
    new_user_message = {
        "role": "user",
        "content": [{"type": "text", "text": prompt}],
    }
    st.session_state.messages.append(new_user_message)
    with st.chat_message("user"):
        user_msg_index = len(st.session_state.messages) - 1
        display_message(new_user_message["content"], user_msg_index)

    # Show progress indicator inside analyst chat message while waiting for response
    with st.chat_message("analyst"):
        with st.spinner("Waiting for Analyst's response..."):

            response, error_msg = get_analyst_response(st.session_state.messages)
            #st.write(response)

            if error_msg is None:
                analyst_message = {
                    "role": "analyst",
                    "content": response,
                    "request_id": response[0]["request_id"],
                }
            else:
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": response["request_id"],
                }
                st.session_state["fire_API_error_notify"] = True

            if "warnings" in response:
                st.session_state.warnings = response["warnings"]

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
    cleaned_reponse = re.sub(r"event: [\s\w\n.:]*", "", response_string)
    parsed_list = [json.loads(x) for x in cleaned_reponse.split("\n") if x != ""]

    text_delta = []
    suggestions_delta = []
    messages = []
    request_id = None
    error_code = None
    request_id = None
    sql_statement = None
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
        elif "sql" in each:
            sql = each["sql"]["statement_delta"]
        else:
            other = each

    rebuilt_response = [{ "type" : "text"
                        , "text" : "".join(text_delta)
                        , "suggestions" : suggestions_delta
                        , "request_id": request_id
                        , "messages" : messages
                        , "error_code" : error_code
                        , "request_id" : request_id
                        , "sql" : sql_statement
                        , "other" : other
                        }]
    
    #st.header(rebuilt_response)

    return rebuilt_response


def get_analyst_response(messages):
    """
    Send chat history to the Cortex Analyst API and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    st.write(f"session_state.message: {st.session_state.messages}")
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": st.session_state.messages,
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

    parsed_content = parsed_response_message(resp.content)

    # Check if the response is successful
    if resp.status_code < 400:
        # Return the content of the response as a JSON object
        return parsed_content, None
    else:
        # Craft readable error message
        error_msg = f"""
                        🚨 An Analyst API error has occurred 🚨
        """
        return parsed_content, error_msg


def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    """
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            if role == "analyst":
                display_message(content, idx, message["request_id"])
            else:
                display_message(content, idx)


def display_message(content, message_index, request_id=""):
    """
    Display a single message content.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message.
    """

    st.header(content)

    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])

        if "suggestions" in item and item["suggestions"]:
            # Display suggestions as buttons
            for suggestion_index, suggestion in enumerate(item["suggestions"]):
                st.button(suggestion["suggestion_delta"], key = f"suggestion_delta_{suggestion_index}")
                #if st.button(
                #    suggestion, key=f"suggestion_{message_index}_{suggestion_index}"
                #):
                 #   st.session_state.active_suggestion = suggestion

        if "sql" in item and item["sql"]:
            # Display the SQL query and results
            display_sql_query(
                item["statement"], message_index, item["confidence"], request_id
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
            st.text(f"Name: {verified_query_used['name']}")
            st.text(f"Question: {verified_query_used['question']}")
            st.text(f"Verified by: {verified_query_used['verified_by']}")
            st.text(
                f"Verified at: {datetime.fromtimestamp(verified_query_used['verified_at'])}"
            )
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
    if request_id:
        display_feedback_section(request_id)


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
        🚨 An Analyst API error has occurred 🚨
        
        * response code: `{resp['status']}`
        * request-id: `{parsed_content['request_id']}`
        * error code: `{parsed_content['error_code']}`
        
        Message:
        ```
        {parsed_content['message']}
        ```
        """
    return err_msg


if __name__ == "__main__":
    main()

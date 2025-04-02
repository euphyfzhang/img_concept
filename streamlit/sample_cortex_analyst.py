# Public Docs: https://docs.snowflake.com/LIMITEDACCESS/snowflake-cortex/rest-api/cortex-analyst

import json
import re
from typing import Any, Generator, Iterator

import pandas
import pandas as pd
import requests
from snowflake.snowpark import Session
import sseclient
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
    display_warnings()


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
        "Welcome to Cortex Analyst! Type your questions below to interact with your data. "
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

def stream(events):
    prev_index = -1
    prev_type = ""
    prev_suggestion_index = -1
    while True:
        event = next(events, None)
        if not event:
            return
        data = json.loads(event.data)
        new_content_block = event.event != "message.content.delta" or data["index"] != prev_index

        if prev_type == "sql" and new_content_block:
            # Close sql markdown once sql section finishes.
            yield "\n```\n\n"
        match event.event:
            case "message.content.delta":
                match data["type"]:
                    case "sql":
                        if new_content_block:
                            # Add sql markdown when we enter a new sql block.
                            yield "```sql\n"
                        yield data["statement_delta"]
                    case "text":
                        yield data["text_delta"]
                    case "suggestions":
                        if new_content_block:
                            # Add a suggestions header when we enter a new suggestions block.
                            yield "\nHere are some example questions you could ask:\n\n"
                            yield "\n- "
                        elif (
                            prev_suggestion_index != data["suggestions_delta"]["index"]
                        ):
                            yield "\n- "
                        yield data["suggestions_delta"]["suggestion_delta"]
                        prev_suggestion_index = data["suggestions_delta"]["index"]
                prev_index = data["index"]
                prev_type = data["type"]
            case "status":
                st.session_state.status = data["status_message"]
                # We return here to allow the spinner to update with the latest status, but this method will be
                #  called again for the next iteration
                return
            case "error":
                st.session_state.error = data
                return

def parsed_response_message(response):
    content = []

    response_string = response.decode("utf-8")
    cleaned_reponse = re.sub(r"event: [\s\w\n.:]*", "", response_string)
    parsed_list = [json.loads(x) for x in cleaned_reponse.split("\n") if x != ""]

    return parsed_list


def process_user_input(prompt: str):
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
        "stream" : True
    }
    st.session_state.messages.append(new_user_message)
    with st.chat_message("user"):
        user_msg_index = len(st.session_state.messages) - 1
        display_message(new_user_message["content"])

    # Show progress indicator inside analyst chat message while waiting for response

    with st.chat_message("analyst"):
        with st.spinner("Waiting for Analyst's response..."):

            written_content, error_msg = get_analyst_response(st.session_state.messages)

            if error_msg is None:
                analyst_message = {
                    "role": "analyst",
                    "content": written_content,
                    "request_id": {written_content[0]["request_id"]}
                }
            else:
                analyst_message = {
                    "role": "analyst",
                    "content": [{"type": "text", "text": error_msg}],
                    "request_id": {written_content[0]["request_id"]}
                }
                st.session_state["fire_API_error_notify"] = True

            #if "warnings" in response:
                #st.session_state.warnings = response["warnings"]

            st.session_state.messages.append(analyst_message)
            st.rerun()


def display_warnings():
    """
    Display warnings to the user.
    """
    warnings = st.session_state.warnings
    for warning in warnings:
        st.warning(warning["message"], icon="⚠️")


def get_analyst_response(messages):
    """
    Send chat history to the Cortex Analyst API and return the response.

    Args:
        messages (List[Dict]): The conversation history.

    Returns:
        Optional[Dict]: The response from the Cortex Analyst API.
    """
    # Prepare the request body with the user's prompt
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{SEMANTIC_FILE}",
        "stream": True,
    }

    # Send a POST request to the Cortex Analyst API endpoint
    # Adjusted to use positional arguments as per the API's requirement
    response = requests.post(
        url=f"https://{st.session_state.CONN.host}/api/v2/cortex/analyst/message",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
        stream=True,
    )

    # Check if the response is successful
    if response.status_code < 400:
        # Return the content of the response as a JSON object
        content = parsed_response_message(response.content)
        return content, None
    else:
        # Craft readable error message
        error_msg = f"""
                        🚨 An Analyst API error has occurred 🚨
                        Message:
                        ```
                        {response.text}
                        ```
                    """
        return response.status_code, error_msg


def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    """
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            if role == "analyst":
                display_message(content, message["request_id"])
            else:
                display_message(content)


def display_message(content, request_id=""):
    """
    Display a single message content.

    Args:
        content (List[Dict[str, str]]): The message content.
        message_index (int): The index of the message.

    """

    text_delta = []
    suggestions = []

    for item in content:
        if "type" in item and item["type"] == "text":
            if "text_delta" in item:
                text_delta.append(item["text_delta"])
            else:
                text = item["text"]
            
        elif "type" in item and item["type"] == "suggestions":
            suggestions.append(item["suggestions_delta"])

        elif "type" in item and item["type"] == "sql":
            # Display the SQL query and results
            display_sql_query(
                item["statement"], message_index, item["confidence"], request_id
            )
        else:
            # Handle other content types if necessary
            pass
    
    if text_delta:
        text = ''.join(text_delta)

    if text:
        st.markdown(text)
    
    if suggestions:
        # Display suggestions as buttons
        for index, suggestion in enumerate(suggestions):
            st.button(suggestion, key=f"suggestion_{index}")


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

    resp = requests.post(
        url=f"https://{st.session_state.CONN.host}/api/v2/cortex/analyst/feedback",
        json=request_body,
        headers={
            "Authorization": f'Snowflake Token="{st.session_state.CONN.rest.token}"',
            "Content-Type": "application/json",
        },
        stream=True,
    )

    if resp.status_code == 200:
        return None

    parsed_content = json.loads(resp)
    # Craft readable error message
    err_msg = f"""
        🚨 An Analyst API error has occurred 🚨
        
        * response code: `{resp.status_code}`
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
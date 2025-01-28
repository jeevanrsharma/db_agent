import json
import pymysql
from phi.agent import Agent
from phi.model.ollama import Ollama

# llm = Groq(
#     id="llama-3.3-70b-versatile",
#     api_key="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" , # Replace with your actual API key
# )

class MySQLTool:
    def __init__(self, connection, schema):
        """
        Custom MySQL Tool to handle database queries.
        :param connection: Active MySQL connection object.
        :param schema: The schema dictionary to interpret SQL queries.
        """
        self.connection = connection
        self.schema = schema
        self.name = "MySQLTool"

    def execute(self, sql_query):
        """
        Executes an SQL query and returns results for SELECT queries
        or confirmation for DML queries.
        :param sql_query: The SQL query string.
        :return: Query results, confirmation message, or an error message.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql_query)
                if sql_query.strip().lower().startswith("select"):
                    return cursor.fetchall()  # Return query results for SELECT queries
                else:
                    self.connection.commit()  # Commit for DML queries (INSERT, UPDATE, DELETE)
                    return {"message": f"Query executed: {sql_query}"}
        except Exception as e:
            return {"error": str(e)}

    def generate_sql(self, prompt):
        """
        Generates a SQL query based on the user's prompt using the schema.
        :param prompt: User prompt containing a natural language query.
        :return: SQL query string.
        """
        # Handle DELETE query
        if "delete" in prompt.lower():
            if "today" in prompt.lower():
                return "DELETE FROM leads_data WHERE created_date = CURDATE()"

        # Handle SELECT query for specific lead_id (e.g., "show details of lead_id 202010150001")
        if "show details of" in prompt.lower():
            # Parse the lead_id from the prompt
            parts = prompt.split("lead_id")
            lead_id = parts[1].strip() if len(parts) > 1 else None
            if lead_id:
                return f"SELECT * FROM leads_data WHERE id = {lead_id}"

        # Default SELECT query
        if "select" in prompt.lower():
            return "SELECT * FROM leads_data"
        
        return {"error": "Unable to parse the query."}

    def __call__(self, prompt):
        """
        Interprets the user prompt and executes SQL queries.
        :param prompt: User prompt containing an SQL query.
        :return: Query results, confirmation message, or an error message.
        """
        sql_query = self.generate_sql(prompt)
        print(f"Generated SQL Query: {sql_query}")  # Debug the SQL query
        if isinstance(sql_query, dict) and "error" in sql_query:
            return sql_query
        return self.execute(sql_query)


# Step 1: Establish MySQL Connection
def create_mysql_connection():
    return pymysql.connect(
        host="127.0.0.1",         # Replace with your MySQL host
        user="root",              # Replace with your MySQL username
        password="",              # Replace with your MySQL password
        database="connectors",    # Replace with your database name
        cursorclass=pymysql.cursors.DictCursor  # Return results as dictionaries
    )


# Step 2: Load Schema from JSON
def load_schema():
    with open('schemas/schema.json') as f:
        return json.load(f)


# Step 3: Initialize Local Ollama LLM
llm = Ollama(
    id="llama3:latest",  # Replace with your locally served model
    # base_url="http://localhost:11434",  # Default Ollama local service
)

# Step 4: Initialize MySQLTool with schema
schema = load_schema()
connection = create_mysql_connection()
mysql_tool = MySQLTool(connection=connection, schema=schema)

# Step 5: Initialize Agent with MySQLTool
db_agent = Agent(
    name="Database Agent",
    model=llm,
    tools=[mysql_tool],  # Use the custom MySQL tool
    instructions=[
        "Analyze the user's question and map it to an appropriate SQL query.",
        "Use the database schema to construct accurate queries.",
        "Execute the query and Respond with results fetched from the database in a user-friendly format.",
    ],
    show_tool_calls=True,
    markdown=True,
)

# Step 6: Example Query
user_question = "Show me the connector code of lead_id 202010150001 from leads_data"
response = db_agent.print_response(user_question, stream=True)

print("Response:", response)  # Display the result of the query
